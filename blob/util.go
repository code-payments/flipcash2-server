package blob

// SupportedImageMimeTypes is the set of MIME types a client may declare for an
// upload. It is derived from the decodable formats, so the type pinned into the
// upload policy is always one the server can re-derive and validate from the
// stored bytes. It is the single source of truth for "is this an image we
// accept".
var SupportedImageMimeTypes = func() map[string]bool {
	set := make(map[string]bool, len(imageFormatToMimeType))
	for _, mimeType := range imageFormatToMimeType {
		set[mimeType] = true
	}
	return set
}()
