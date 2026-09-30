package redact

import (
	"encoding/binary"
	"errors"

	blobpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/blob/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	"google.golang.org/protobuf/proto"
)

// Content returns a placeholder for a message's content: the same kind of
// content with the same structure — a text of the same shape, a reply to
// the same message, an image with the same caption shape, a cash card in
// the same denomination — and none of what the sender put in it. The input
// is never modified.
//
// The placeholder is built field by field rather than by clearing the
// original, so every field is an allowlist decision and a field added to the
// proto later is dropped until one is made for it. Per kind:
//
//   - Text: the text becomes Text(seed, text).
//   - Reply: the replied-to message ID, a per-chat sequence number, is
//     kept so the client can quote the message it points at — the quoted
//     message is redacted on its own. The body is redacted like a message.
//   - Media: every rendition is kept with its blob ID and metadata — mime
//     type, size, and for an image its dimensions and blurhash, which is
//     the blurred preview — less the download URL, a bearer credential for
//     the bytes. The blob ID is a handle the blob service checks access on,
//     so it gives a viewer nothing. Metadata without a download URL is what
//     the proto defines for a viewer who may know what a blob is but not
//     fetch it (see BlobMetadata.download_url): the client renders the
//     blurhash and does not ask for a URL. The caption is redacted like
//     text.
//   - Cash: kept as is. A payment is public on chain, so a cash card shows
//     nothing a viewer could not learn there.
//   - Deleted: kept as is. It carries no content, and who deleted a message
//     is the same kind of fact as who sent one, which is not this
//     function's to hide.
//   - System: kept as is. A system message is the server's account of what
//     happened in the chat — a join, a departure — not something a member
//     said, and the proto contract (Message.redacted) lists it with cash
//     and deleted content as never redacted. Its structure is still
//     settling; a variant that carries member-authored text is a new
//     decision here, not a pass-through.
//   - Encrypted: kept as is. The server cannot read the ciphertext, so it
//     has no shape to build a placeholder from, and nothing it could hide:
//     only the DM's two members hold the key. Encrypted content is DM-only
//     and a DM's only readers are its members, so the one viewer who reaches
//     this is a member reading REDACTED, who could decrypt it anyway.
//   - Widget: the same variant, built field by field like the content that
//     holds it. A ShareProfileWidget's username becomes redactedUsername, so
//     the viewer sees a profile was shared but not whose. A variant this
//     function does not know is ErrUnsupportedContent.
//
// A nil content, or one of a kind this function does not know, is
// ErrUnsupportedContent: nothing unknown is passed through, and the caller
// decides what to do with a message it cannot show.
//
// The chat and message IDs seed every text in the content (see messageSeed), so
// the same message renders the same placeholder wherever it is read.
func Content(chatID *commonpb.ChatId, messageID *messagingpb.MessageId, c *messagingpb.Content) (*messagingpb.Content, error) {
	return content(messageSeed(chatID, messageID), c)
}

// ErrUnsupportedContent is returned by Content for content it defines no
// placeholder for.
var ErrUnsupportedContent = errors.New("redact: unsupported content")

// messageSeed identifies a message across the whole deployment. A message ID is a
// per-chat sequence number, so the chat ID goes first; its length is fixed
// by the chat type (see chat IDs), so the two cannot run into each other.
func messageSeed(chatID *commonpb.ChatId, messageID *messagingpb.MessageId) []byte {
	out := make([]byte, 0, len(chatID.GetValue())+8)
	out = append(out, chatID.GetValue()...)
	return binary.BigEndian.AppendUint64(out, messageID.GetValue())
}

func content(seed []byte, c *messagingpb.Content) (*messagingpb.Content, error) {
	if c == nil {
		return nil, ErrUnsupportedContent
	}
	switch t := c.Type.(type) {
	case *messagingpb.Content_Text:
		return &messagingpb.Content{Type: &messagingpb.Content_Text{
			Text: textContent(seed, t.Text),
		}}, nil
	case *messagingpb.Content_Reply:
		reply, err := replyContent(seed, t.Reply)
		if err != nil {
			return nil, err
		}
		return &messagingpb.Content{Type: &messagingpb.Content_Reply{Reply: reply}}, nil
	case *messagingpb.Content_Media:
		return &messagingpb.Content{Type: &messagingpb.Content_Media{
			Media: mediaContent(seed, t.Media),
		}}, nil
	case *messagingpb.Content_Cash:
		return &messagingpb.Content{Type: &messagingpb.Content_Cash{
			Cash: proto.Clone(t.Cash).(*messagingpb.CashContent),
		}}, nil
	case *messagingpb.Content_Deleted:
		return &messagingpb.Content{Type: &messagingpb.Content_Deleted{
			Deleted: proto.Clone(t.Deleted).(*messagingpb.DeletedContent),
		}}, nil
	case *messagingpb.Content_System:
		return &messagingpb.Content{Type: &messagingpb.Content_System{
			System: proto.Clone(t.System).(*messagingpb.SystemContent),
		}}, nil
	case *messagingpb.Content_Encrypted:
		return &messagingpb.Content{Type: &messagingpb.Content_Encrypted{
			Encrypted: proto.Clone(t.Encrypted).(*messagingpb.EncryptedContent),
		}}, nil
	case *messagingpb.Content_Widget:
		widget, err := widgetContent(t.Widget)
		if err != nil {
			return nil, err
		}
		return &messagingpb.Content{Type: &messagingpb.Content_Widget{Widget: widget}}, nil
	default:
		return nil, ErrUnsupportedContent
	}
}

func textContent(seed []byte, t *messagingpb.TextContent) *messagingpb.TextContent {
	return &messagingpb.TextContent{Text: Text(seed, t.GetText())}
}

func replyContent(seed []byte, r *messagingpb.ReplyContent) (*messagingpb.ReplyContent, error) {
	out := &messagingpb.ReplyContent{
		RepliedMessageId: proto.Clone(r.GetRepliedMessageId()).(*messagingpb.MessageId),
	}
	for _, c := range r.GetContent() {
		redacted, err := content(seed, c)
		if err != nil {
			return nil, err
		}
		out.Content = append(out.Content, redacted)
	}
	return out, nil
}

func mediaContent(seed []byte, m *messagingpb.MediaContent) *messagingpb.MediaContent {
	out := &messagingpb.MediaContent{}
	for _, item := range m.GetItems() {
		media := &blobpb.Media{}
		for _, r := range item.GetRenditions() {
			redacted := proto.Clone(r).(*blobpb.Rendition)
			if redacted.Blob != nil {
				redacted.Blob.DownloadUrl = nil
			}
			media.Renditions = append(media.Renditions, redacted)
		}
		out.Items = append(out.Items, media)
	}
	if m.GetCaption() != nil {
		out.Caption = textContent(seed, m.GetCaption())
	}
	return out
}

// redactedUsername stands in for the username a redacted widget names. It is
// a valid handle (see commonpb.Username), so the placeholder passes the same
// validation as the original; the message's redacted flag is what tells the
// client not to act on it.
const redactedUsername = "redacted"

func widgetContent(w *messagingpb.WidgetContent) (*messagingpb.WidgetContent, error) {
	switch w.GetType().(type) {
	case *messagingpb.WidgetContent_ShareProfile:
		return &messagingpb.WidgetContent{Type: &messagingpb.WidgetContent_ShareProfile{
			ShareProfile: &messagingpb.ShareProfileWidget{
				Username: &commonpb.Username{Value: redactedUsername},
			},
		}}, nil
	default:
		return nil, ErrUnsupportedContent
	}
}
