package dynamodb

import (
	"context"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/code-payments/flipcash2-server/cluster"
)

// The cluster store spans three tables:
//
//	cluster_members  pk = instance_id (one item per live process incarnation).
//	          The heartbeat counter is advanced by its owner and observed by
//	          peers; liveness is counter movement against the observer's own
//	          clock, never a wall-clock comparison.
//
//	cluster_claims   pk = "<namespace>#<hex key>" (one item per claimed key).
//	          A single-attribute pk spreads claims across partitions — keying
//	          by namespace would pile every chat claim onto one partition.
//	          Claim rows are PERMANENT — released claims keep their item
//	          (owner cleared) and carry no ttl attribute, so the per-key
//	          fence is unconditionally monotonic. A ttl here would be the
//	          one thing able to delete a held claim under a live owner
//	          (nothing refreshes a hot key's row — steady-state serving is
//	          deliberately write-free), handing the key to a vacant acquire
//	          with no evidence check and a reset fence. Row count is bounded
//	          by distinct keys ever claimed; if that ever matters, reclaim
//	          space with an explicit sweep of vacated rows, never a ttl.
//
//	cluster_subscriptions  pk = "<namespace>#<hex key>", sk = instance_id (one
//	          item per interested server per topic). Non-exclusive interest
//	          rows: plain upserts and deletes, no conditions, no fence. Like
//	          claims they carry NO ttl attribute — a row is held for as long
//	          as its streams live with zero refreshing writes, so a ttl would
//	          eventually expire a live subscriber's row and silently stop
//	          delivery to it. Validity is the member's liveness; cleanup is
//	          explicit (drains delete their own rows, observers sweep crashed
//	          instances' rows at resolution time).
//
// A takeover is a TransactWriteItems pairing the claim update with a
// ConditionCheck that the displaced owner's heartbeat counter still equals the
// caller's stale observation — the evidence of death must still be true,
// atomically, at commit time.
const (
	attrInstanceID = "instance_id"
	attrAddress    = "address"
	attrLabels     = "labels"
	attrDraining   = "draining"
	attrHeartbeat  = "heartbeat_counter"
	attrTTL        = "ttl"

	attrPK           = "pk"
	attrNamespace    = "ns"
	attrKey          = "key_bytes"
	attrOwner        = "owner_instance"
	attrOwnerAddress = "owner_address"
	attrFence        = "fence"

	// memberItemTTL bounds how long a dead member's row lingers before
	// DynamoDB's lazy TTL sweep removes it. GC only — every read path
	// re-checks freshness itself, and the membership layer actively deletes
	// corpse records after MemberGCAfter, so this is the backstop behind the
	// backstop. It is safe on member rows ONLY because every heartbeat
	// refreshes it: a live member's row can never expire out from under it.
	// Claim rows have no analogous refresh and therefore no ttl at all.
	memberItemTTL = time.Hour
)

type store struct {
	client             *dynamodb.Client
	membersTable       string
	claimsTable        string
	subscriptionsTable string
}

// NewInDynamoDB returns a cluster.Store backed by the given DynamoDB tables.
// Use CreateTables to provision them.
func NewInDynamoDB(client *dynamodb.Client, membersTable, claimsTable, subscriptionsTable string) cluster.Store {
	return &store{
		client:             client,
		membersTable:       membersTable,
		claimsTable:        claimsTable,
		subscriptionsTable: subscriptionsTable,
	}
}

// claimPK also keys subscription topics — both tables address (namespace,
// key) with the same encoding.
func claimPK(namespace string, key []byte) string {
	return namespace + "#" + hex.EncodeToString(key)
}

func ttlValue(ttl time.Duration) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(time.Now().Add(ttl).Unix(), 10)}
}

func (s *store) PutMember(ctx context.Context, member *cluster.Member, heartbeatCounter uint64) error {
	labels := make(map[string]types.AttributeValue, len(member.Labels))
	for k, v := range member.Labels {
		labels[k] = &types.AttributeValueMemberS{Value: v}
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.membersTable),
		Item: map[string]types.AttributeValue{
			attrInstanceID: &types.AttributeValueMemberS{Value: member.InstanceID},
			attrAddress:    &types.AttributeValueMemberS{Value: member.Address},
			attrLabels:     &types.AttributeValueMemberM{Value: labels},
			attrDraining:   &types.AttributeValueMemberBOOL{Value: member.Draining},
			attrHeartbeat:  &types.AttributeValueMemberN{Value: strconv.FormatUint(heartbeatCounter, 10)},
			attrTTL:        ttlValue(memberItemTTL),
		},
	})
	return err
}

func (s *store) Heartbeat(ctx context.Context, instanceID string) (uint64, error) {
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.membersTable),
		Key: map[string]types.AttributeValue{
			attrInstanceID: &types.AttributeValueMemberS{Value: instanceID},
		},
		ConditionExpression: aws.String("attribute_exists(#id)"),
		UpdateExpression:    aws.String("ADD #hb :one SET #ttl = :ttl"),
		ExpressionAttributeNames: map[string]string{
			"#id":  attrInstanceID,
			"#hb":  attrHeartbeat,
			"#ttl": attrTTL,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{Value: "1"},
			":ttl": ttlValue(memberItemTTL),
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	if err != nil {
		var conditionFailed *types.ConditionalCheckFailedException
		if errors.As(err, &conditionFailed) {
			return 0, cluster.ErrMemberNotFound
		}
		return 0, err
	}
	return parseUint(out.Attributes[attrHeartbeat])
}

func (s *store) SetDraining(ctx context.Context, instanceID string, draining bool) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.membersTable),
		Key: map[string]types.AttributeValue{
			attrInstanceID: &types.AttributeValueMemberS{Value: instanceID},
		},
		ConditionExpression: aws.String("attribute_exists(#id)"),
		UpdateExpression:    aws.String("SET #draining = :draining"),
		ExpressionAttributeNames: map[string]string{
			"#id":       attrInstanceID,
			"#draining": attrDraining,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":draining": &types.AttributeValueMemberBOOL{Value: draining},
		},
	})
	if err != nil {
		var conditionFailed *types.ConditionalCheckFailedException
		if errors.As(err, &conditionFailed) {
			return cluster.ErrMemberNotFound
		}
		return err
	}
	return nil
}

func (s *store) DeleteMember(ctx context.Context, instanceID string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.membersTable),
		Key: map[string]types.AttributeValue{
			attrInstanceID: &types.AttributeValueMemberS{Value: instanceID},
		},
	})
	return err
}

func (s *store) GetMembers(ctx context.Context) ([]*cluster.MemberRecord, error) {
	var out []*cluster.MemberRecord
	var startKey map[string]types.AttributeValue
	for {
		resp, err := s.client.Scan(ctx, &dynamodb.ScanInput{
			TableName:         aws.String(s.membersTable),
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}

		for _, item := range resp.Items {
			record, err := memberRecordFromItem(item)
			if err != nil {
				return nil, err
			}
			out = append(out, record)
		}

		if len(resp.LastEvaluatedKey) == 0 {
			return out, nil
		}
		startKey = resp.LastEvaluatedKey
	}
}

func (s *store) AcquireClaim(ctx context.Context, namespace string, key []byte, self *cluster.Member, takeover *cluster.TakeoverTarget) (*cluster.Claim, error) {
	if takeover != nil {
		return s.acquireByTakeover(ctx, namespace, key, self, takeover)
	}
	return s.acquireVacant(ctx, namespace, key, self)
}

// acquireVacant acquires an absent or released claim, incrementing the fence.
// An owner's re-acquire is detected on the conditional failure and returned
// with the fence unchanged.
func (s *store) acquireVacant(ctx context.Context, namespace string, key []byte, self *cluster.Member) (*cluster.Claim, error) {
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.claimsTable),
		Key: map[string]types.AttributeValue{
			attrPK: &types.AttributeValueMemberS{Value: claimPK(namespace, key)},
		},
		ConditionExpression: aws.String("attribute_not_exists(#pk) OR #owner = :empty"),
		UpdateExpression:    aws.String("SET #ns = :ns, #key = :key, #owner = :me, #addr = :addr ADD #fence :one"),
		ExpressionAttributeNames: map[string]string{
			"#pk":    attrPK,
			"#ns":    attrNamespace,
			"#key":   attrKey,
			"#owner": attrOwner,
			"#addr":  attrOwnerAddress,
			"#fence": attrFence,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ns":    &types.AttributeValueMemberS{Value: namespace},
			":key":   &types.AttributeValueMemberB{Value: key},
			":me":    &types.AttributeValueMemberS{Value: self.InstanceID},
			":addr":  &types.AttributeValueMemberS{Value: self.Address},
			":empty": &types.AttributeValueMemberS{Value: ""},
			":one":   &types.AttributeValueMemberN{Value: "1"},
		},
		ReturnValues:                        types.ReturnValueAllNew,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	if err == nil {
		return claimFromItem(out.Attributes)
	}

	var conditionFailed *types.ConditionalCheckFailedException
	if !errors.As(err, &conditionFailed) {
		return nil, err
	}

	holder, parseErr := claimFromItem(conditionFailed.Item)
	if parseErr != nil {
		// The failed-condition item wasn't returned; fall back to a read.
		holder, parseErr = s.GetClaim(ctx, namespace, key)
		if errors.Is(parseErr, cluster.ErrClaimNotFound) {
			// Raced a release between the write and the read: contention,
			// retriable.
			return nil, cluster.ErrClaimHeld
		} else if parseErr != nil {
			// A real store failure must surface as itself, not as contention.
			return nil, parseErr
		}
	}
	if holder.OwnerInstanceID == self.InstanceID {
		return holder, nil
	}
	return holder, cluster.ErrClaimHeld
}

// acquireByTakeover displaces the claim from a presumed-dead holder: one
// transaction updates the claim iff the holder still holds it AND the holder's
// member record is absent or its heartbeat counter still equals the caller's
// stale observation.
func (s *store) acquireByTakeover(ctx context.Context, namespace string, key []byte, self *cluster.Member, takeover *cluster.TakeoverTarget) (*cluster.Claim, error) {
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				ConditionCheck: &types.ConditionCheck{
					TableName: aws.String(s.membersTable),
					Key: map[string]types.AttributeValue{
						attrInstanceID: &types.AttributeValueMemberS{Value: takeover.InstanceID},
					},
					ConditionExpression: aws.String("attribute_not_exists(#id) OR #hb = :observed"),
					ExpressionAttributeNames: map[string]string{
						"#id": attrInstanceID,
						"#hb": attrHeartbeat,
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":observed": &types.AttributeValueMemberN{Value: strconv.FormatUint(takeover.HeartbeatCounter, 10)},
					},
				},
			},
			{
				Update: &types.Update{
					TableName: aws.String(s.claimsTable),
					Key: map[string]types.AttributeValue{
						attrPK: &types.AttributeValueMemberS{Value: claimPK(namespace, key)},
					},
					ConditionExpression: aws.String("#owner = :target"),
					UpdateExpression:    aws.String("SET #owner = :me, #addr = :addr ADD #fence :one"),
					ExpressionAttributeNames: map[string]string{
						"#owner": attrOwner,
						"#addr":  attrOwnerAddress,
						"#fence": attrFence,
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":target": &types.AttributeValueMemberS{Value: takeover.InstanceID},
						":me":     &types.AttributeValueMemberS{Value: self.InstanceID},
						":addr":   &types.AttributeValueMemberS{Value: self.Address},
						":one":    &types.AttributeValueMemberN{Value: "1"},
					},
				},
			},
		},
	})
	if err != nil {
		var canceled *types.TransactionCanceledException
		if !errors.As(err, &canceled) {
			return nil, err
		}
		if !canceledByConditionFailure(canceled) {
			// Throttling and transaction conflicts cancel transactions too;
			// those are store failures (or transient contention on the
			// transact machinery), not evidence going stale — surfacing them
			// as ErrClaimHeld would redirect the caller back to the corpse.
			return nil, err
		}
		// Evidence went stale (the holder heartbeated — it was alive) or the
		// claim changed hands. If it was released in the meantime, a vacant
		// acquire settles it; otherwise report the current holder.
		holder, getErr := s.GetClaim(ctx, namespace, key)
		if errors.Is(getErr, cluster.ErrClaimNotFound) {
			return s.acquireVacant(ctx, namespace, key, self)
		} else if getErr != nil {
			return nil, getErr
		}
		if holder.OwnerInstanceID == self.InstanceID {
			return holder, nil
		}
		return holder, cluster.ErrClaimHeld
	}

	// TransactWriteItems returns no item values; read the committed claim
	// back. Anyone who displaced us in the interleaving is reported instead.
	claim, err := s.GetClaim(ctx, namespace, key)
	if err != nil {
		return nil, err
	}
	if claim.OwnerInstanceID != self.InstanceID {
		return claim, cluster.ErrClaimHeld
	}
	return claim, nil
}

func (s *store) GetClaim(ctx context.Context, namespace string, key []byte) (*cluster.Claim, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.claimsTable),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			attrPK: &types.AttributeValueMemberS{Value: claimPK(namespace, key)},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, cluster.ErrClaimNotFound
	}
	claim, err := claimFromItem(out.Item)
	if err != nil {
		return nil, err
	}
	if claim.OwnerInstanceID == "" {
		return nil, cluster.ErrClaimNotFound
	}
	return claim, nil
}

func (s *store) ReleaseClaim(ctx context.Context, namespace string, key []byte, instanceID string) error {
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.claimsTable),
		Key: map[string]types.AttributeValue{
			attrPK: &types.AttributeValueMemberS{Value: claimPK(namespace, key)},
		},
		ConditionExpression: aws.String("#owner = :me"),
		UpdateExpression:    aws.String("SET #owner = :empty, #addr = :empty"),
		ExpressionAttributeNames: map[string]string{
			"#owner": attrOwner,
			"#addr":  attrOwnerAddress,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":me":    &types.AttributeValueMemberS{Value: instanceID},
			":empty": &types.AttributeValueMemberS{Value: ""},
		},
	})
	if err != nil {
		var conditionFailed *types.ConditionalCheckFailedException
		if errors.As(err, &conditionFailed) {
			return nil // Not held by instanceID: release is a no-op.
		}
		return err
	}
	return nil
}

func (s *store) PutSubscription(ctx context.Context, namespace string, key []byte, member *cluster.Member) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.subscriptionsTable),
		Item: map[string]types.AttributeValue{
			attrPK:         &types.AttributeValueMemberS{Value: claimPK(namespace, key)},
			attrInstanceID: &types.AttributeValueMemberS{Value: member.InstanceID},
			attrNamespace:  &types.AttributeValueMemberS{Value: namespace},
			attrKey:        &types.AttributeValueMemberB{Value: key},
			attrAddress:    &types.AttributeValueMemberS{Value: member.Address},
		},
	})
	return err
}

func (s *store) DeleteSubscription(ctx context.Context, namespace string, key []byte, instanceID string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.subscriptionsTable),
		Key: map[string]types.AttributeValue{
			attrPK:         &types.AttributeValueMemberS{Value: claimPK(namespace, key)},
			attrInstanceID: &types.AttributeValueMemberS{Value: instanceID},
		},
	})
	return err
}

func (s *store) GetSubscribers(ctx context.Context, namespace string, key []byte) ([]*cluster.Subscription, error) {
	var out []*cluster.Subscription
	var startKey map[string]types.AttributeValue
	for {
		resp, err := s.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(s.subscriptionsTable),
			ConsistentRead:         aws.Bool(true),
			KeyConditionExpression: aws.String("#pk = :pk"),
			ExpressionAttributeNames: map[string]string{
				"#pk": attrPK,
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: claimPK(namespace, key)},
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}

		for _, item := range resp.Items {
			sub, err := subscriptionFromItem(item)
			if err != nil {
				return nil, err
			}
			out = append(out, sub)
		}

		if len(resp.LastEvaluatedKey) == 0 {
			return out, nil
		}
		startKey = resp.LastEvaluatedKey
	}
}

func subscriptionFromItem(item map[string]types.AttributeValue) (*cluster.Subscription, error) {
	sub := &cluster.Subscription{}
	if v, ok := item[attrInstanceID].(*types.AttributeValueMemberS); ok {
		sub.InstanceID = v.Value
	} else {
		return nil, errors.New("subscription item missing instance_id")
	}
	if v, ok := item[attrNamespace].(*types.AttributeValueMemberS); ok {
		sub.Namespace = v.Value
	}
	if v, ok := item[attrKey].(*types.AttributeValueMemberB); ok {
		sub.Key = append([]byte(nil), v.Value...)
	}
	if v, ok := item[attrAddress].(*types.AttributeValueMemberS); ok {
		sub.Address = v.Value
	}
	return sub, nil
}

// canceledByConditionFailure reports whether any of the transaction's
// cancellation reasons is a failed condition (as opposed to throttling or a
// transaction conflict).
func canceledByConditionFailure(canceled *types.TransactionCanceledException) bool {
	for _, reason := range canceled.CancellationReasons {
		if aws.ToString(reason.Code) == "ConditionalCheckFailed" {
			return true
		}
	}
	return false
}

func memberRecordFromItem(item map[string]types.AttributeValue) (*cluster.MemberRecord, error) {
	record := &cluster.MemberRecord{
		Member: cluster.Member{Labels: make(map[string]string)},
	}

	if v, ok := item[attrInstanceID].(*types.AttributeValueMemberS); ok {
		record.InstanceID = v.Value
	} else {
		return nil, errors.New("member item missing instance_id")
	}
	if v, ok := item[attrAddress].(*types.AttributeValueMemberS); ok {
		record.Address = v.Value
	}
	if v, ok := item[attrDraining].(*types.AttributeValueMemberBOOL); ok {
		record.Draining = v.Value
	}
	if v, ok := item[attrLabels].(*types.AttributeValueMemberM); ok {
		for k, lv := range v.Value {
			if s, ok := lv.(*types.AttributeValueMemberS); ok {
				record.Labels[k] = s.Value
			}
		}
	}

	counter, err := parseUint(item[attrHeartbeat])
	if err != nil {
		return nil, err
	}
	record.HeartbeatCounter = counter
	return record, nil
}

func claimFromItem(item map[string]types.AttributeValue) (*cluster.Claim, error) {
	if len(item) == 0 {
		return nil, cluster.ErrClaimNotFound
	}

	claim := &cluster.Claim{}
	if v, ok := item[attrNamespace].(*types.AttributeValueMemberS); ok {
		claim.Namespace = v.Value
	}
	if v, ok := item[attrKey].(*types.AttributeValueMemberB); ok {
		claim.Key = append([]byte(nil), v.Value...)
	}
	if v, ok := item[attrOwner].(*types.AttributeValueMemberS); ok {
		claim.OwnerInstanceID = v.Value
	}
	if v, ok := item[attrOwnerAddress].(*types.AttributeValueMemberS); ok {
		claim.OwnerAddress = v.Value
	}

	fence, err := parseUint(item[attrFence])
	if err != nil {
		return nil, err
	}
	claim.Fence = fence
	return claim, nil
}

func parseUint(v types.AttributeValue) (uint64, error) {
	n, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0, errors.New("attribute is not a number")
	}
	return strconv.ParseUint(n.Value, 10, 64)
}
