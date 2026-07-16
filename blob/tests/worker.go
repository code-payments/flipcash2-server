package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"

	"github.com/code-payments/flipcash2-server/blob"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
)

// putObjectFunc stores bytes directly into the upload store under a key, as if
// a client had finished uploading them. Like uploadFunc it is supplied by the
// caller so the suite depends only on the blob.ObjectStorage interface.
type putObjectFunc func(key string, data []byte)

// RunWorkerTests runs the shared blob.Worker test suite against the given
// metadata store. The object storage is always a fake injected by the caller
// (along with a direct put hook), matching RunServerTests.
func RunWorkerTests(
	t *testing.T,
	blobs blob.Store,
	storage blob.ObjectStorage,
	putObject putObjectFunc,
	teardown func(),
) {
	for _, tf := range []func(t *testing.T, blobs blob.Store, storage blob.ObjectStorage, putObject putObjectFunc){
		testWorkerFinalizesUploadedBlob,
		testWorkerRejectsFlaggedBlob,
		testWorkerRetriesUntilBytesArrive,
		testWorkerExhaustedAttemptsRejectAsInternal,
		testWorkerSkipsClaimedWork,
		testWorkerProcessesBatchAcrossBlobs,
	} {
		tf(t, blobs, storage, putObject)
		teardown()
	}
}

// workerHarness is a worker over the suite's stores, with the pieces tests
// reach into to stage and observe work.
type workerHarness struct {
	worker    *blob.Worker
	blobs     blob.Store
	storage   blob.ObjectStorage
	putObject putObjectFunc
}

func newWorkerHarness(t *testing.T, blobs blob.Store, storage blob.ObjectStorage, putObject putObjectFunc, moderator moderation.Client, opts ...blob.WorkerOption) *workerHarness {
	log := zaptest.NewLogger(t)
	finalizer := blob.NewFinalizer(log, blobs, storage, moderator)
	return &workerHarness{
		worker:    blob.NewWorker(log, blobs, finalizer, blob.ContentKindImage, opts...),
		blobs:     blobs,
		storage:   storage,
		putObject: putObject,
	}
}

// stageUpload reserves a pending original for the given bytes and, when
// uploaded is set, stores them as the client's finished upload.
func (h *workerHarness) stageUpload(t *testing.T, data []byte, uploaded bool) *blob.Blob {
	id := blob.MustGenerateID()
	key, err := blob.StorageKey(id, "image/png")
	require.NoError(t, err)

	record := &blob.Blob{
		ID:         id,
		Rendition:  blob.RenditionOriginal,
		Owner:      model.MustGenerateUserID(),
		State:      blob.StatePending,
		StorageKey: key,
		MimeType:   "image/png",
		SizeBytes:  uint64(len(data)),
	}
	require.NoError(t, h.blobs.CreatePending(context.Background(), record))
	if uploaded {
		h.putObject(key, data)
	}
	return record
}

// mark queues the record for finalization, due immediately.
func (h *workerHarness) mark(t *testing.T, record *blob.Blob) {
	require.NoError(t, h.blobs.MarkForFinalization(context.Background(), record.ID, record.ContentKind(), time.Now()))
}

// state re-reads the record's committed state.
func (h *workerHarness) state(t *testing.T, record *blob.Blob) *blob.Blob {
	got, err := h.blobs.GetByID(context.Background(), record.ID)
	require.NoError(t, err)
	return got
}

// process runs one worker tick and asserts how many tasks it took on.
func (h *workerHarness) process(t *testing.T, expected int) {
	processed, err := h.worker.Process(context.Background())
	require.NoError(t, err)
	require.Equal(t, expected, processed)
}

func testWorkerFinalizesUploadedBlob(t *testing.T, blobs blob.Store, storage blob.ObjectStorage, putObject putObjectFunc) {
	h := newWorkerHarness(t, blobs, storage, putObject, nil)
	record := h.stageUpload(t, makePNG(t, 100, 80), true)
	h.mark(t, record)

	h.process(t, 1)

	got := h.state(t, record)
	require.Equal(t, blob.StateReady, got.State)
	require.NotNil(t, got.Image)
	require.NotEmpty(t, got.Renditions)

	// The upload-store bytes are cleaned up once the blob is READY.
	_, err := storage.GetUploaded(context.Background(), record.StorageKey)
	require.ErrorIs(t, err, blob.ErrObjectNotFound)

	// The queue is drained.
	h.process(t, 0)
}

func testWorkerRejectsFlaggedBlob(t *testing.T, blobs blob.Store, storage blob.ObjectStorage, putObject putObjectFunc) {
	h := newWorkerHarness(t, blobs, storage, putObject, &fakeModerator{flagged: true, categories: []string{"general_nsfw"}})
	record := h.stageUpload(t, makePNG(t, 50, 50), true)
	h.mark(t, record)

	h.process(t, 1)

	got := h.state(t, record)
	require.Equal(t, blob.StateRejected, got.State)
	require.NotNil(t, got.Rejection)
	require.Equal(t, blob.RejectionReasonModeration, got.Rejection.Reason)
	require.Equal(t, moderationpb.FlaggedCategory_NSFW, got.Rejection.FlaggedCategory)

	h.process(t, 0)
}

func testWorkerRetriesUntilBytesArrive(t *testing.T, blobs blob.Store, storage blob.ObjectStorage, putObject putObjectFunc) {
	// A vanishing backoff keeps the retried task immediately due, so the test
	// drives attempts with successive Process calls instead of sleeping.
	h := newWorkerHarness(t, blobs, storage, putObject, nil, blob.WithWorkerBackoff(time.Nanosecond, time.Nanosecond))
	data := makePNG(t, 40, 30)
	record := h.stageUpload(t, data, false)
	h.mark(t, record)

	// The bytes are missing, so the attempt fails and is rescheduled with its
	// attempt count bumped.
	h.process(t, 1)
	require.Equal(t, blob.StatePending, h.state(t, record).State)

	due, err := blobs.GetDueForFinalization(context.Background(), blob.ContentKindImage, time.Now().Add(time.Second), 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.EqualValues(t, 1, due[0].Attempts)

	// Once the upload lands, the next attempt finalizes it.
	h.putObject(record.StorageKey, data)
	h.process(t, 1)
	require.Equal(t, blob.StateReady, h.state(t, record).State)
}

func testWorkerExhaustedAttemptsRejectAsInternal(t *testing.T, blobs blob.Store, storage blob.ObjectStorage, putObject putObjectFunc) {
	h := newWorkerHarness(t, blobs, storage, putObject, nil,
		blob.WithWorkerBackoff(time.Nanosecond, time.Nanosecond),
		blob.WithWorkerMaxAttempts(2),
	)
	record := h.stageUpload(t, makePNG(t, 40, 30), false) // bytes never arrive
	h.mark(t, record)

	// Two failed attempts burn the budget; the third tick fails the blob
	// terminally instead of retrying again.
	for range 2 {
		h.process(t, 1)
		require.Equal(t, blob.StatePending, h.state(t, record).State)
	}
	h.process(t, 1)

	got := h.state(t, record)
	require.Equal(t, blob.StateRejected, got.State)
	require.NotNil(t, got.Rejection)
	require.Equal(t, blob.RejectionReasonInternal, got.Rejection.Reason)

	h.process(t, 0)
}

func testWorkerSkipsClaimedWork(t *testing.T, blobs blob.Store, storage blob.ObjectStorage, putObject putObjectFunc) {
	h := newWorkerHarness(t, blobs, storage, putObject, nil)
	record := h.stageUpload(t, makePNG(t, 40, 30), true)
	h.mark(t, record)

	// Another worker instance holds the claim; this one must leave the task
	// alone rather than duplicate the work.
	claimed, err := blobs.ClaimForFinalization(context.Background(), record.ID, time.Now(), time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.True(t, claimed)

	h.process(t, 0)
	require.Equal(t, blob.StatePending, h.state(t, record).State)
}

func testWorkerProcessesBatchAcrossBlobs(t *testing.T, blobs blob.Store, storage blob.ObjectStorage, putObject putObjectFunc) {
	h := newWorkerHarness(t, blobs, storage, putObject, nil, blob.WithWorkerMaxConcurrency(2))
	records := make([]*blob.Blob, 0, 5)
	for range 5 {
		record := h.stageUpload(t, makePNG(t, 60, 40), true)
		h.mark(t, record)
		records = append(records, record)
	}

	h.process(t, 5)
	for _, record := range records {
		require.Equal(t, blob.StateReady, h.state(t, record).State)
	}
}
