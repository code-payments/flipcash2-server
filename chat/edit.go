package chat

import (
	"bytes"
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	chatpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/chat/v1"
	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	eventpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/event/v1"

	"github.com/code-payments/flipcash2-server/model"
)

// EditChat changes any of a group's title, description, profile picture and
// cover picture, atomically.
//
// Only a group can be edited, and only by its creator while they are a member
// (see Chat.PermissionsFor): a DM has no editable record and no creator, a
// group written before creators were recorded has no one who may edit it,
// and a creator who left their group edits nothing until they rejoin. Anyone
// else is DENIED — the same answer ViewerState.Permissions.can_edit gives
// them, so a client never shows an affordance this refuses. Creatorship is
// decided off the record before membership is read, so the store is asked
// nothing on behalf of a caller who could never edit.
//
// A field set to the value the record already holds is dropped before
// anything is checked, so a retried request, or a client that sends every
// field, costs no moderation and no attach — and a request left with nothing
// to change is answered OK from the record as it stands, with nothing written
// and nothing published. What remains is checked in StartChat's order, for
// its reasons: the title through moderation (TITLE_MODERATED), then the
// description (DESCRIPTION_MODERATED; an empty one, which clears it, has
// nothing to judge), then the profile picture attached
// (PROFILE_PICTURE_BLOB_NOT_ACCEPTED), then the cover picture
// (COVER_PICTURE_BLOB_NOT_ACCEPTED), and only once every part has passed is
// the record written, as one update naming exactly those fields. The
// atomicity the proto promises is that: a refusal of any part leaves the
// record as it was, though a picture attached ahead of a write that then
// fails or a later part's refusal stays granted — harmless, as in StartChat.
// A description a group may not carry at all (see ValidateDescription) is
// refused as malformed, with InvalidArgument, before anything is read.
//
// A real change is announced once, on the chat topic, to every member's
// devices including the editor's, as one MetadataUpdate per field changed;
// a picture's is the resolved rendition set the response carries. The
// response is the record after the edit as the editor sees it. A group whose
// record vanished between the read and the write is NOT_FOUND.
func (s *Server) EditChat(ctx context.Context, req *chatpb.EditChatRequest) (*chatpb.EditChatResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	if description := req.GetDescription(); description != nil {
		if err := ValidateDescription(description.GetValue()); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid description")
		}
	}

	if !IsGroupChatID(req.ChatId) {
		return &chatpb.EditChatResponse{Result: chatpb.EditChatResponse_DENIED}, nil
	}

	c, err := s.chats.GetChatByID(ctx, req.ChatId)
	switch {
	case errors.Is(err, ErrChatNotFound):
		return &chatpb.EditChatResponse{Result: chatpb.EditChatResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat")
		return nil, status.Error(codes.Internal, "")
	}
	if c.Type != chatpb.ChatType_GROUP || !c.IsCreator(userID) {
		return &chatpb.EditChatResponse{Result: chatpb.EditChatResponse_DENIED}, nil
	}

	isMember, err := s.access.IsMemberWithChat(ctx, c, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking chat membership")
		return nil, status.Error(codes.Internal, "")
	}
	if !c.PermissionsFor(userID, isMember).CanEdit {
		return &chatpb.EditChatResponse{Result: chatpb.EditChatResponse_DENIED}, nil
	}

	var edit GroupEdit
	if title := req.GetTitle(); title != nil && title.GetValue() != c.Title {
		edit.Title = &title.Value
	}
	if description := req.GetDescription(); description != nil && description.GetValue() != c.Description {
		edit.Description = &description.Value
	}
	if picture := req.GetProfilePicture(); picture != nil && !bytes.Equal(picture.GetBlobId().GetValue(), c.ProfilePictureBlobID.GetValue()) {
		edit.ProfilePictureBlobID = picture.BlobId
	}
	if picture := req.GetCoverPicture(); picture != nil && !bytes.Equal(picture.GetBlobId().GetValue(), c.CoverPictureBlobID.GetValue()) {
		edit.CoverPictureBlobID = picture.BlobId
	}

	if edit.Title != nil {
		flagged, category, err := s.moderateTitle(ctx, log, *edit.Title)
		if err != nil {
			return nil, status.Error(codes.Internal, "")
		}
		if flagged {
			return &chatpb.EditChatResponse{
				Result:          chatpb.EditChatResponse_TITLE_MODERATED,
				FlaggedCategory: category,
			}, nil
		}
	}

	if edit.Description != nil && *edit.Description != "" {
		flagged, category, err := s.moderateDescription(ctx, log, *edit.Description)
		if err != nil {
			return nil, status.Error(codes.Internal, "")
		}
		if flagged {
			return &chatpb.EditChatResponse{
				Result:          chatpb.EditChatResponse_DESCRIPTION_MODERATED,
				FlaggedCategory: category,
			}, nil
		}
	}

	if edit.ProfilePictureBlobID != nil {
		accepted, err := s.attachChatPicture(ctx, log, userID, req.ChatId, edit.ProfilePictureBlobID)
		if err != nil {
			return nil, err
		}
		if !accepted {
			return &chatpb.EditChatResponse{Result: chatpb.EditChatResponse_PROFILE_PICTURE_BLOB_NOT_ACCEPTED}, nil
		}
	}

	if edit.CoverPictureBlobID != nil {
		accepted, err := s.attachChatPicture(ctx, log, userID, req.ChatId, edit.CoverPictureBlobID)
		if err != nil {
			return nil, err
		}
		if !accepted {
			return &chatpb.EditChatResponse{Result: chatpb.EditChatResponse_COVER_PICTURE_BLOB_NOT_ACCEPTED}, nil
		}
	}

	if !edit.IsEmpty() {
		err := s.chats.EditGroup(ctx, req.ChatId, edit)
		switch {
		case errors.Is(err, ErrChatNotFound):
			return &chatpb.EditChatResponse{Result: chatpb.EditChatResponse_NOT_FOUND}, nil
		case err != nil:
			log.With(zap.Error(err)).Warn("Failure editing chat")
			return nil, status.Error(codes.Internal, "")
		}
		// The record in hand is the one the write produced: the edit named
		// exactly these fields, and hydrate reads the record it is given.
		if edit.Title != nil {
			c.Title = *edit.Title
		}
		if edit.Description != nil {
			c.Description = *edit.Description
		}
		if edit.ProfilePictureBlobID != nil {
			c.ProfilePictureBlobID = edit.ProfilePictureBlobID
		}
		if edit.CoverPictureBlobID != nil {
			c.CoverPictureBlobID = edit.CoverPictureBlobID
		}
	}

	metadata, err := s.hydrate(ctx, userID, memberListenerStanding, ReadingFull, fullDetail, []*Chat{c})
	if err != nil {
		// The edit has landed; only the read back failed. A retry finds every
		// field already set and answers from the record.
		log.With(zap.Error(err)).Warn("Failure hydrating chat metadata")
		return nil, status.Error(codes.Internal, "")
	}
	md := metadata[0]

	if !edit.IsEmpty() {
		s.publishMetadataEdited(req.ChatId, edit, md)
	}

	return &chatpb.EditChatResponse{
		Result: chatpb.EditChatResponse_OK,
		Chat:   md,
	}, nil
}

// publishMetadataEdited announces an edit of a group's record on the chat
// topic — one event carrying a MetadataUpdate per field the edit changed, in
// the order the proto declares them — to every member's devices, the editor's
// included: a group update publishes once on its topic and never loads the
// roster (see publishRosterUpdate for the one exception, a transition, whose
// subject's own streams are not yet or no longer on it). Each picture carried
// is the hydrated one, with the renditions the server derived, so a client
// applies it as it would the metadata's. Best-effort and non-blocking, like
// every publish here.
func (s *Server) publishMetadataEdited(chatID *commonpb.ChatId, edit GroupEdit, md *chatpb.Metadata) {
	var updates []*chatpb.MetadataUpdate
	if edit.Title != nil {
		updates = append(updates, &chatpb.MetadataUpdate{
			Kind: &chatpb.MetadataUpdate_TitleChanged_{
				TitleChanged: &chatpb.MetadataUpdate_TitleChanged{NewTitle: *edit.Title},
			},
		})
	}
	if edit.ProfilePictureBlobID != nil {
		updates = append(updates, &chatpb.MetadataUpdate{
			Kind: &chatpb.MetadataUpdate_ProfilePictureChanged_{
				ProfilePictureChanged: &chatpb.MetadataUpdate_ProfilePictureChanged{NewProfilePicture: md.GetProfilePicture()},
			},
		})
	}
	if edit.Description != nil {
		updates = append(updates, &chatpb.MetadataUpdate{
			Kind: &chatpb.MetadataUpdate_DescriptionChanged_{
				DescriptionChanged: &chatpb.MetadataUpdate_DescriptionChanged{NewDescription: *edit.Description},
			},
		})
	}
	if edit.CoverPictureBlobID != nil {
		updates = append(updates, &chatpb.MetadataUpdate{
			Kind: &chatpb.MetadataUpdate_CoverPictureChanged_{
				CoverPictureChanged: &chatpb.MetadataUpdate_CoverPictureChanged{NewCoverPicture: md.GetCoverPicture()},
			},
		})
	}
	s.chatEventBus.OnEvent(chatID, &eventpb.ChatEvent{
		ChatId: chatID,
		Event: &eventpb.Event{
			Id: model.MustGenerateEventID(),
			Ts: timestamppb.Now(),
			Type: &eventpb.Event_ChatUpdate{ChatUpdate: &eventpb.ChatUpdate{
				Chat:            chatID,
				MetadataUpdates: updates,
			}},
		},
	})
}
