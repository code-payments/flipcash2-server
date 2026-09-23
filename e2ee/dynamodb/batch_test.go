package dynamodb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// throttledDynamo stands in for DynamoDB's HTTP endpoint and answers every
// BatchWriteItem and BatchGetItem with the whole request left unprocessed
// (a partition over its budget) for the first `partial` calls, then with
// everything processed. It never returns an error, so the SDK's own retry
// stays out of the picture; only the store's loops are exercised.
type throttledDynamo struct {
	mu      sync.Mutex
	partial int
	calls   int
	at      []time.Time
	// onCall, if set, runs on every call, before the answer.
	onCall func(calls int)
}

func (f *throttledDynamo) Do(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.at = append(f.at, time.Now())
	if f.onCall != nil {
		f.onCall(f.calls)
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	var in struct {
		RequestItems json.RawMessage `json:"RequestItems"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, err
	}

	out := map[string]json.RawMessage{}
	if f.calls <= f.partial {
		switch req.Header.Get("X-Amz-Target") {
		case "DynamoDB_20120810.BatchWriteItem":
			out["UnprocessedItems"] = in.RequestItems
		case "DynamoDB_20120810.BatchGetItem":
			out["Responses"] = json.RawMessage(`{}`)
			out["UnprocessedKeys"] = in.RequestItems
		}
	}
	resp, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(bytes.NewReader(resp)),
		Request:    req,
	}, nil
}

func newThrottledClient(f *throttledDynamo) *dynamodb.Client {
	return dynamodb.New(dynamodb.Options{
		Region:                          "us-east-1",
		Credentials:                     credentials.NewStaticCredentialsProvider("test", "test", ""),
		BaseEndpoint:                    aws.String("http://dynamodb.invalid"),
		HTTPClient:                      f,
		Retryer:                         aws.NopRetryer{},
		DisableValidateResponseChecksum: true,
	})
}

// shrinkBatchBackoff makes the waits short enough for a unit test and
// returns the least the loop must have waited before its nth call.
func shrinkBatchBackoff(t *testing.T) func(calls int) time.Duration {
	base, ceiling := batchBackoffBase, batchBackoffMax
	batchBackoffBase, batchBackoffMax = 2*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { batchBackoffBase, batchBackoffMax = base, ceiling })
	return func(calls int) time.Duration {
		var total time.Duration
		wait := batchBackoffBase
		for i := 1; i < calls; i++ {
			total += wait
			wait = min(wait*2, batchBackoffMax)
		}
		return total
	}
}

func TestE2ee_DynamoDBStore_BatchWrite_BacksOffUnprocessed(t *testing.T) {
	minWait := shrinkBatchBackoff(t)
	f := &throttledDynamo{partial: 3}
	client := newThrottledClient(f)

	start := time.Now()
	err := batchPut(context.Background(), client, "t", []map[string]types.AttributeValue{
		{attrPK: avS("user#x"), attrSK: avS("dev#n#otk#g#1")},
	})
	require.NoError(t, err)
	require.Equal(t, 4, f.calls, "three partial rounds, then the one that drains")
	require.GreaterOrEqual(t, time.Since(start), minWait(4), "the loop waited between rounds")
	require.Less(t, f.at[1].Sub(f.at[0]), f.at[3].Sub(f.at[2]), "the wait grew")
}

func TestE2ee_DynamoDBStore_BatchWrite_GivesUpAfterItsRounds(t *testing.T) {
	shrinkBatchBackoff(t)
	f := &throttledDynamo{partial: 1 << 20}
	client := newThrottledClient(f)

	err := batchPut(context.Background(), client, "t", []map[string]types.AttributeValue{
		{attrPK: avS("user#x"), attrSK: avS("dev#n#otk#g#1")},
	})
	require.ErrorIs(t, err, errBatchUnprocessed)
	require.Equal(t, maxBatchRounds, f.calls)
}

func TestE2ee_DynamoDBStore_BatchWrite_HonorsContext(t *testing.T) {
	shrinkBatchBackoff(t)
	f := &throttledDynamo{partial: 1 << 20}
	client := newThrottledClient(f)

	// The caller's deadline ends during the wait after the first round: the
	// loop returns at once with the context's error, never a further call.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f.onCall = func(int) { cancel() }
	err := batchPut(ctx, client, "t", []map[string]types.AttributeValue{
		{attrPK: avS("user#x"), attrSK: avS("dev#n#otk#g#1")},
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, f.calls, "the first round is issued, the wait before the second is what ends it")
}

// A delivery's batches are issued side by side and each drains its own
// leftovers: sixty puts are three batches, the endpoint leaves the first
// three calls wholly unprocessed, and every batch lands on its second try.
func TestE2ee_DynamoDBStore_BatchWriteConcurrently_DrainsEachBatch(t *testing.T) {
	shrinkBatchBackoff(t)
	f := &throttledDynamo{partial: 3}
	client := newThrottledClient(f)

	requests := make([]types.WriteRequest, 0, 60)
	for i := range 60 {
		requests = append(requests, types.WriteRequest{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
			attrPK: avS("mbox#" + string(rune('a'+i%3))),
			attrSK: avS(envelopeSK(uint64(i))),
		}}})
	}
	err := batchWriteConcurrently(context.Background(), client, "t", requests, deliverConcurrency)
	require.NoError(t, err)
	require.Equal(t, 6, f.calls, "three batches, each refused once then drained")
}

// One batch that never drains fails the whole call.
func TestE2ee_DynamoDBStore_BatchWriteConcurrently_GivesUp(t *testing.T) {
	shrinkBatchBackoff(t)
	f := &throttledDynamo{partial: 1 << 20}
	client := newThrottledClient(f)

	requests := []types.WriteRequest{{PutRequest: &types.PutRequest{Item: map[string]types.AttributeValue{
		attrPK: avS("mbox#a"), attrSK: avS(envelopeSK(1)),
	}}}}
	err := batchWriteConcurrently(context.Background(), client, "t", requests, deliverConcurrency)
	require.ErrorIs(t, err, errBatchUnprocessed)
}
