package memory

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"

	"github.com/code-payments/flipcash2-server/blob"
)

const (
	// These stand in for a presigned S3 POST endpoint and a CloudFront
	// distribution. The object key travels in the POST form's "key" field, the
	// way a real S3 POST policy carries it.
	uploadURL         = "https://upload.blobs.test/"
	downloadURLPrefix = "https://cdn.blobs.test/"

	uploadTTL   = 15 * time.Minute
	downloadTTL = 15 * time.Minute
)

// Storage is an in-memory blob.ObjectStorage for tests. It stands in for the
// two-bucket S3 + CloudFront backend: uploaded bytes land in the upload store,
// Promote moves validated bytes into the origin store, and SignDownloadURL
// mints a fake CDN URL for the served bytes.
type Storage struct {
	sync.Mutex

	uploaded map[string][]byte
	served   map[string][]byte
}

// NewInMemoryStorage returns an in-memory blob.ObjectStorage for tests.
func NewInMemoryStorage() *Storage {
	return &Storage{
		uploaded: make(map[string][]byte),
		served:   make(map[string][]byte),
	}
}

func (s *Storage) PresignUpload(_ context.Context, key, mimeType string, sizeBytes uint64) (*blobpb.UploadTarget, error) {
	return &blobpb.UploadTarget{
		Method: blobpb.UploadTarget_POST,
		Url:    uploadURL,
		FormFields: map[string]string{
			"key":          key,
			"Content-Type": mimeType,
		},
		ExpiresAt: timestamppb.New(time.Now().Add(uploadTTL)),
	}, nil
}

func (s *Storage) GetUploaded(_ context.Context, key string) ([]byte, error) {
	s.Lock()
	defer s.Unlock()

	data, ok := s.uploaded[key]
	if !ok {
		return nil, blob.ErrObjectNotFound
	}
	return append([]byte(nil), data...), nil
}

func (s *Storage) UploadExists(_ context.Context, key string) (bool, error) {
	s.Lock()
	defer s.Unlock()

	_, ok := s.uploaded[key]
	return ok, nil
}

func (s *Storage) CopyToOrigin(_ context.Context, key string) error {
	s.Lock()
	defer s.Unlock()

	if data, ok := s.uploaded[key]; ok {
		s.served[key] = data
		return nil
	}
	// A repeated finalization may copy after the upload bytes were already
	// cleaned up; if the object is already served, that is a no-op success.
	if _, ok := s.served[key]; ok {
		return nil
	}
	return blob.ErrObjectNotFound
}

func (s *Storage) PutOrigin(_ context.Context, key, _ string, data []byte) error {
	s.Lock()
	defer s.Unlock()

	// Derived bytes land directly in the served (origin) store, never the upload
	// store — they are server-produced, not client-uploaded.
	s.served[key] = append([]byte(nil), data...)
	return nil
}

func (s *Storage) DeleteUpload(_ context.Context, key string) error {
	s.Lock()
	defer s.Unlock()

	delete(s.uploaded, key)
	return nil
}

func (s *Storage) SignDownloadURL(_ context.Context, key string) (*blobpb.DownloadUrl, error) {
	return &blobpb.DownloadUrl{
		Url:       downloadURLPrefix + key,
		ExpiresAt: timestamppb.New(time.Now().Add(downloadTTL)),
	}, nil
}

// SimulateUpload stores bytes as if the client had POSTed them to the presigned
// target, keyed by the target's "key" form field.
func (s *Storage) SimulateUpload(target *blobpb.UploadTarget, data []byte) {
	s.PutObject(target.FormFields["key"], data)
}

// PutObject stores bytes directly into the upload store, bypassing the presign
// dance.
func (s *Storage) PutObject(key string, data []byte) {
	s.Lock()
	defer s.Unlock()

	s.uploaded[key] = append([]byte(nil), data...)
}

func (s *Storage) reset() {
	s.Lock()
	defer s.Unlock()

	s.uploaded = make(map[string][]byte)
	s.served = make(map[string][]byte)
}

var _ blob.ObjectStorage = (*Storage)(nil)
