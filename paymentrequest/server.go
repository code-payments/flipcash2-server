package paymentrequest

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	messagingpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/messaging/v1"
	moderationpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/moderation/v1"
	paymentrequestpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/paymentrequest/v1"

	currency_lib "github.com/code-payments/ocp-server/currency"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/chat"
	"github.com/code-payments/flipcash2-server/model"
	"github.com/code-payments/flipcash2-server/moderation"
	"github.com/code-payments/flipcash2-server/send"
)

const (
	// maxPageSize bounds a single GetChatPaymentRequests page and is its
	// default. It matches the max_items on
	// GetChatPaymentRequestsResponse.payment_requests.
	maxPageSize = 100

	pageTokenVersion = 1

	// pageTokenLen is the fixed size of a GetChatPaymentRequests paging token:
	// the version byte, the section byte and the chat ID the token is bound
	// to, then the position it resumes after (see PageCursor): the sort time
	// as nanoseconds and the request ID.
	pageTokenLen = 1 + 1 + groupChatIDSize + 8 + IDSize
)

// Server implements paymentrequestpb.PaymentRequestServer.
//
// Who may do what in a group chat request is the chat's to say, through the
// shared chat.Access, except where the request itself names who:
//
//   - Creating a request is speaking in the chat (Access.SpeakerStanding), and
//     the standing also says whether the chat takes a split's text in
//     plaintext (a public group) or encrypted under the chat key (a keyed
//     private group). For now creation is also staff-only, and public groups
//     are refused outright (see CreatePaymentRequest); both are transitional.
//   - Seeing a request, alone or in its chat's listing, is reading the chat in
//     full (Access.CanListen): its members, and a non-member of a public
//     group who satisfies its listener rules.
//   - Closing a request and marking a share paid by hand are its creator's
//     alone, whatever their standing in the chat now: the request is theirs,
//     and they are the one who is paid.
//
// Paying a share with a Flipcash payment is not this service's: it is an
// intent, validated and recorded elsewhere (see Store.PayShare).
type Server struct {
	log *zap.Logger

	authz auth.Authorizer

	requests  Store
	accounts  account.Store
	chats     chat.Store
	access    *chat.Access
	moderator moderation.Client

	now func() time.Time

	// allowPublicGroups lifts the refusal of public groups on creation (see
	// CreatePaymentRequest). Only tests set it (see WithPublicGroupRequests).
	allowPublicGroups bool

	paymentrequestpb.UnimplementedPaymentRequestServer
}

// ServerOption configures a Server at construction beyond its required
// dependencies.
type ServerOption func(*Server)

// WithClock overrides the clock the server decides a request's status and
// deadlines by, for tests.
func WithClock(now func() time.Time) ServerOption {
	return func(s *Server) {
		s.now = now
	}
}

// WithPublicGroupRequests lifts the transitional refusal of requests in public
// groups (see CreatePaymentRequest), so the plaintext path stays built and
// tested while it is closed. It is for tests only: no production server sets
// it, and opening public groups is deleting the check, not setting this.
func WithPublicGroupRequests() ServerOption {
	return func(s *Server) {
		s.allowPublicGroups = true
	}
}

// NewServer returns a PaymentRequest service over requests. access is the
// chat.Access the chat and messaging servers share, so a request is gated by
// exactly what gates the chat it belongs to; chats is read for what Access
// does not answer (a group's existence, and whether a split's debtors are its
// members), and accounts for the staff gate on creation.
func NewServer(
	log *zap.Logger,

	authz auth.Authorizer,

	requests Store,
	accounts account.Store,
	chats chat.Store,
	access *chat.Access,
	moderator moderation.Client,

	opts ...ServerOption,
) *Server {
	s := &Server{
		log: log,

		authz: authz,

		requests:  requests,
		accounts:  accounts,
		chats:     chats,
		access:    access,
		moderator: moderator,

		now: time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// CreatePaymentRequest creates a payment request. Today the only kind is a
// split in a group chat.
//
// The request is checked in the order a client can act on, local checks
// first, the store and chat reads next, and moderation, a call out, last:
// the caller must be staff (DENIED, see canCreate); the chat must be a group
// (CHAT_NOT_SUPPORTED); the caller must be able to speak in it (DENIED); it
// must not be a public group (CHAT_NOT_SUPPORTED, transitional, see below);
// the text must be in the form the chat takes, plaintext in a public group
// and encrypted under the chat key in a private one (ENCRYPTION_NOT_ALLOWED,
// ENCRYPTION_REQUIRED); ends_at must be in the future and at most MaxDuration
// away (INVALID_ENDS_AT); the
// currency must be one the server values (INVALID_CURRENCY); the shares must
// be valid (INVALID_SHARES, see validateShares); and plaintext must pass
// moderation (TEXT_MODERATED, see moderate). Only then is anything written.
//
// A chat that does not exist is DENIED, not reported: the proto has no
// NOT_FOUND for creation, and Access.SpeakerStanding admits no one to a chat
// nothing is stored under, so a missing group is refused exactly as a group
// the caller is not in.
//
// The proto supports public groups; they are refused for now, a transitional
// gate removed by deleting this check (see WithPublicGroupRequests). The
// plaintext path below is built and tested so lifting it changes nothing
// else.
//
// The RPC is retry-safe. The request's ID is derived from the caller and the
// request's idempotency key (see MustDeriveID), so a retry names the same
// request, and one that already exists is answered from the store before any
// check runs, whatever the retry carries: a staff flag since revoked, a
// member since departed or a text the moderator has since learned to flag
// are facts about a new request, not this one. A retry that loses a race
// with its twin is caught by the store's uniqueness condition and answered
// the same way.
func (s *Server) CreatePaymentRequest(ctx context.Context, req *paymentrequestpb.CreatePaymentRequestRequest) (*paymentrequestpb.CreatePaymentRequestResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	// Validation requires the key at its fixed width, which MustDeriveID
	// relies on.
	id := MustDeriveID(userID, req.IdempotencyKey)

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("payment_request_id", hex.EncodeToString(id.Value)),
	)

	existing, err := s.requests.Get(ctx, id)
	switch {
	case err == nil:
		return createdResponse(existing, s.now()), nil
	case !errors.Is(err, ErrNotFound):
		log.With(zap.Error(err)).Warn("Failure getting payment request")
		return nil, status.Error(codes.Internal, "")
	}

	allowed, err := s.canCreate(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure checking staff status")
		return nil, status.Error(codes.Internal, "")
	}
	if !allowed {
		return &paymentrequestpb.CreatePaymentRequestResponse{Result: paymentrequestpb.CreatePaymentRequestResponse_DENIED}, nil
	}

	var groupChat *paymentrequestpb.GroupChatPaymentRequest
	switch kind := req.Kind.(type) {
	case *paymentrequestpb.CreatePaymentRequestRequest_GroupChat:
		groupChat = kind.GroupChat
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported payment request kind")
	}
	chatID := groupChat.ChatId
	log = log.With(zap.String("chat_id", hex.EncodeToString(chatID.GetValue())))

	var split *paymentrequestpb.Split
	switch typ := groupChat.Type.(type) {
	case *paymentrequestpb.GroupChatPaymentRequest_Split:
		split = typ.Split
	default:
		return nil, status.Error(codes.InvalidArgument, "unsupported payment request type")
	}

	// A DM is told apart by its ID's length, before anything is read.
	if !chat.IsGroupChatID(chatID) {
		return &paymentrequestpb.CreatePaymentRequestResponse{Result: paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED}, nil
	}

	speaker, err := s.access.SpeakerStanding(ctx, chatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining speaker standing")
		return nil, status.Error(codes.Internal, "")
	}
	if !speaker.CanSpeak {
		return &paymentrequestpb.CreatePaymentRequestResponse{Result: paymentrequestpb.CreatePaymentRequestResponse_DENIED}, nil
	}

	// Public groups are refused for now, whoever asks (see above). Only a
	// public group takes plaintext alone.
	if speaker.Encryption == chat.EncryptionNone && !s.allowPublicGroups {
		return &paymentrequestpb.CreatePaymentRequestResponse{Result: paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED}, nil
	}

	if result, ok := textVerdict(speaker, split); !ok {
		return &paymentrequestpb.CreatePaymentRequestResponse{Result: result}, nil
	}

	now := s.now()
	if untilEnd := split.EndsAt.AsTime().Sub(now); untilEnd <= 0 || untilEnd > MaxDuration {
		return &paymentrequestpb.CreatePaymentRequestResponse{Result: paymentrequestpb.CreatePaymentRequestResponse_INVALID_ENDS_AT}, nil
	}

	if !isSupportedCurrency(split.Currency) {
		return &paymentrequestpb.CreatePaymentRequestResponse{Result: paymentrequestpb.CreatePaymentRequestResponse_INVALID_CURRENCY}, nil
	}

	r, err := NewSplitRequest(id, userID, chatID, split, now)
	if errors.Is(err, ErrInvalidShares) {
		return &paymentrequestpb.CreatePaymentRequestResponse{Result: paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES}, nil
	}
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure building payment request")
		return nil, status.Error(codes.Internal, "")
	}

	validShares, err := s.validateShares(ctx, r)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure validating shares")
		return nil, status.Error(codes.Internal, "")
	}
	if !validShares {
		return &paymentrequestpb.CreatePaymentRequestResponse{Result: paymentrequestpb.CreatePaymentRequestResponse_INVALID_SHARES}, nil
	}

	if text := split.GetPlaintext(); text != nil {
		result, category, err := s.moderate(ctx, log, text)
		if err != nil {
			return nil, status.Error(codes.Internal, "")
		}
		if result != paymentrequestpb.CreatePaymentRequestResponse_OK {
			return &paymentrequestpb.CreatePaymentRequestResponse{
				Result:          result,
				FlaggedCategory: category,
			}, nil
		}
	}

	err = s.requests.Create(ctx, r)
	switch {
	case errors.Is(err, ErrExists):
		// A concurrent retry won the write since the read above. Its request is
		// this request's request, so answer with it.
		existing, err := s.requests.Get(ctx, id)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting payment request created by concurrent request")
			return nil, status.Error(codes.Internal, "")
		}
		return createdResponse(existing, s.now()), nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure creating payment request")
		return nil, status.Error(codes.Internal, "")
	}

	return createdResponse(r, now), nil
}

// createdResponse answers a CreatePaymentRequest OK with r as a reader sees it
// at now.
func createdResponse(r *PaymentRequest, now time.Time) *paymentrequestpb.CreatePaymentRequestResponse {
	return &paymentrequestpb.CreatePaymentRequestResponse{
		Result:         paymentrequestpb.CreatePaymentRequestResponse_OK,
		PaymentRequest: r.ToProto(now),
	}
}

// canCreate reports whether userID may create a payment request: today, only a
// staff user. It is a transitional gate, like chat.Server's on private group
// creation: payment requests are being built in steps behind it, and clients
// have not shipped them. Opening creation to everyone is removing this check.
func (s *Server) canCreate(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	return s.accounts.IsStaff(ctx, userID)
}

// textVerdict judges a split's text against what the chat takes, as the
// speaker's standing says (see chat.SpeakerStanding): a public group takes
// plaintext alone, and a keyed private group text encrypted under its scheme
// alone. ok is false, with the result to answer, when the chat does not take
// the text.
func textVerdict(speaker chat.SpeakerStanding, split *paymentrequestpb.Split) (result paymentrequestpb.CreatePaymentRequestResponse_Result, ok bool) {
	switch speaker.Encryption {
	case chat.EncryptionNone:
		if split.GetPlaintext() == nil {
			return paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_NOT_ALLOWED, false
		}
		return paymentrequestpb.CreatePaymentRequestResponse_OK, true
	case chat.EncryptionRequired:
		encrypted := split.GetEncrypted()
		if encrypted == nil {
			return paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_REQUIRED, false
		}
		if scheme, ok := textScheme(speaker.Scheme); !ok || encrypted.Scheme != scheme {
			return paymentrequestpb.CreatePaymentRequestResponse_ENCRYPTION_NOT_ALLOWED, false
		}
		return paymentrequestpb.CreatePaymentRequestResponse_OK, true
	default:
		// Only a DM takes optional encryption, and a DM is refused by its ID
		// before any standing is asked for.
		return paymentrequestpb.CreatePaymentRequestResponse_CHAT_NOT_SUPPORTED, false
	}
}

// textScheme is the EncryptedPaymentRequestText scheme that corresponds to a
// chat's content scheme: a split's text in a private group is encrypted with
// the same chat key as its messages.
func textScheme(scheme messagingpb.EncryptedContent_Scheme) (paymentrequestpb.EncryptedPaymentRequestText_Scheme, bool) {
	switch scheme {
	case messagingpb.EncryptedContent_CHAT_KEY_XCHACHA20POLY1305:
		return paymentrequestpb.EncryptedPaymentRequestText_CHAT_KEY_XCHACHA20POLY1305, true
	default:
		return paymentrequestpb.EncryptedPaymentRequestText_UNKNOWN, false
	}
}

// isSupportedCurrency reports whether the server can value amounts in code:
// when Flipcash offers it for payments (see send.PresetsFor). Those are the
// currencies clients pay in, and so ones OCP prices; a debtor paying their
// share pays in the request's currency. The list is deterministic, unlike
// asking OCP to value an amount. It repeats poll's check rather than import
// it.
func isSupportedCurrency(code string) bool {
	_, ok := send.PresetsFor(currency_lib.Code(code))
	return ok
}

// validateShares checks what NewSplitRequest cannot judge alone of r's shares
// (it has already refused a share with paid set, a repeated user, and shares
// naming no one but the creator): that every amount is a whole number, at
// least one, of the currency's minimum transfer value (its minor unit), and
// that every debtor is a member of the chat.
//
// A share must be an amount a payment can pay exactly. A payment of it is
// accepted within half a minor unit of the share (see intent's
// validateRequestedPaymentAppMetadata), so a share on a whole minor unit
// leaves exactly one whole-unit payment in that window, the share itself,
// where a share between units (1.004, or 0.005) would accept a payment short of
// it. A client dividing a bill rounds each share to the unit itself. Only float
// error is forgiven: an amount within a few ulps of a whole number of units
// (0.1 + 0.2 for 0.30, say) passes. An amount that is not finite is refused.
//
// Membership is read for every debtor at once with Store.GetGroupMembersByID,
// which is eventually consistent. That is accepted for a validation: the
// proto already lets a share name someone who was a member when the request
// was created and has since left, and a member who has just joined but is
// refused by a stale read is a retry away from being accepted. The creator's
// own share needs no read: their standing to speak has proved them a member.
func (s *Server) validateShares(ctx context.Context, r *PaymentRequest) (bool, error) {
	minimum := send.MinimumTransferValue(currency_lib.Code(r.Split.Currency))
	debtors := make([]*commonpb.UserId, 0, len(r.Shares()))
	for _, share := range r.Shares() {
		if !isWholeMinorUnits(share.NativeAmount, minimum) {
			return false, nil
		}
		if !proto.Equal(share.UserId, r.Creator) {
			debtors = append(debtors, share.UserId)
		}
	}

	members, err := s.chats.GetGroupMembersByID(ctx, r.ChatID, debtors)
	if err != nil {
		return false, err
	}
	for _, debtor := range debtors {
		if _, ok := members[string(debtor.Value)]; !ok {
			return false, nil
		}
	}
	return true, nil
}

// isWholeMinorUnits reports whether amount is a whole number, at least one, of
// the minor unit minimum, to within a few ulps of amount (see validateShares).
func isWholeMinorUnits(amount, minimum float64) bool {
	if math.IsNaN(amount) || math.IsInf(amount, 0) {
		return false
	}
	units := math.Round(amount / minimum)
	if units < 1 {
		return false
	}
	return math.Abs(amount-units*minimum) <= 4*(math.Nextafter(amount, math.Inf(1))-amount)
}

// moderate runs a split's plaintext through the general text classifier as one
// text, the title and then the description on a line of its own, and returns
// the result to answer: OK, or TEXT_MODERATED and the best-fit category.
//
// Any failure to classify is an error, and nothing is created. That includes
// ErrUnsupportedLanguage, as for a poll (see poll.Server.CreatePoll): there is
// no second classifier to cover the text, so a language the classifier cannot
// score is refused rather than posted unmoderated.
func (s *Server) moderate(ctx context.Context, log *zap.Logger, text *paymentrequestpb.PaymentRequestText) (paymentrequestpb.CreatePaymentRequestResponse_Result, moderationpb.FlaggedCategory, error) {
	result, err := s.moderator.ClassifyText(ctx, moderationText(text))
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure classifying payment request text")
		return paymentrequestpb.CreatePaymentRequestResponse_OK, moderationpb.FlaggedCategory_NONE, err
	}
	if result == nil || !result.Flagged {
		return paymentrequestpb.CreatePaymentRequestResponse_OK, moderationpb.FlaggedCategory_NONE, nil
	}
	log.With(zap.String("text", moderationText(text)), zap.Strings("categories", result.FlaggedCategories)).Info("Payment request text is flagged")
	return paymentrequestpb.CreatePaymentRequestResponse_TEXT_MODERATED, moderation.HighestFlaggedCategory(result), nil
}

// moderationText is the one text a split's plaintext is moderated as: the
// title, then the description on its own line when there is one.
func moderationText(text *paymentrequestpb.PaymentRequestText) string {
	lines := []string{text.Title}
	if text.Description != "" {
		lines = append(lines, text.Description)
	}
	return strings.Join(lines, "\n")
}

// GetPaymentRequest returns a request as a reader sees it now.
//
// A request that does not exist is NOT_FOUND, checked before the caller's
// standing; a request ID is derived from its creator and a key only they
// hold, so the distinction leaks nothing. Otherwise the caller must be able to
// read the request's chat in full (DENIED).
//
// A caller holding an unpaid share of the request is also given the intent ID
// their payment of it must use (PaymentRequest.PaymentIntentIDs), whatever
// the request's status: a payment's validation judges that. No one else is
// ever given it, the creator included.
func (s *Server) GetPaymentRequest(ctx context.Context, req *paymentrequestpb.GetPaymentRequestRequest) (*paymentrequestpb.GetPaymentRequestResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("payment_request_id", hex.EncodeToString(req.GetPaymentRequestId().GetValue())),
	)

	r, err := s.requests.Get(ctx, req.PaymentRequestId)
	switch {
	case errors.Is(err, ErrNotFound):
		return &paymentrequestpb.GetPaymentRequestResponse{Result: paymentrequestpb.GetPaymentRequestResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting payment request")
		return nil, status.Error(codes.Internal, "")
	}

	canListen, err := s.access.CanListen(ctx, r.ChatID, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining listener standing")
		return nil, status.Error(codes.Internal, "")
	}
	if !canListen {
		return &paymentrequestpb.GetPaymentRequestResponse{Result: paymentrequestpb.GetPaymentRequestResponse_DENIED}, nil
	}

	resp := &paymentrequestpb.GetPaymentRequestResponse{
		Result:         paymentrequestpb.GetPaymentRequestResponse_OK,
		PaymentRequest: r.ToProto(s.now()),
	}
	if share := r.Share(userID); share != nil && share.GetPaid() == nil {
		if intentID := r.PaymentIntentID(userID); intentID != nil {
			resp.PaymentIntentId = &commonpb.IntentId{Value: append([]byte(nil), intentID...)}
		}
	}
	return resp, nil
}

// GetChatPaymentRequests pages one section of a group chat's requests (see
// Store.GetChatPage for each section's order).
//
// A DM is DENIED before anything is read, a token that is malformed or bound
// to another chat or section is InvalidArgument, a group that does not exist
// is NOT_FOUND, and the caller must be able to read it in full (DENIED). The
// group's existence is told apart by reading its rules (Store.GetGroupRules,
// which a store caches) and then asking Access with them, since Access alone
// admits no one to a missing chat without saying so.
//
// The open section is by stored status, so it can hold a request past its
// ends_at that the expiry worker has not yet stored expired. Such a request is
// EXPIRED to every reader and is dropped from the page, and the store is read
// on until the page is full or it runs dry, so a backlog of them never yields
// an empty page that claims more.
func (s *Server) GetChatPaymentRequests(ctx context.Context, req *paymentrequestpb.GetChatPaymentRequestsRequest) (*paymentrequestpb.GetChatPaymentRequestsResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("chat_id", hex.EncodeToString(req.GetChatId().GetValue())),
	)

	if !chat.IsGroupChatID(req.ChatId) {
		return &paymentrequestpb.GetChatPaymentRequestsResponse{Result: paymentrequestpb.GetChatPaymentRequestsResponse_DENIED}, nil
	}

	section, ok := sectionFromProto(req.Section)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid section")
	}

	limit := maxPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	// A token names the chat and section it was minted for: one replayed into
	// another would be a valid position there, so it is refused rather than
	// resumed.
	var after *PageCursor
	if token := req.GetQueryOptions().GetPagingToken(); token != nil {
		cursor, ok := decodePageToken(token, req.ChatId, section)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid paging token")
		}
		after = cursor
	}

	rules, err := s.chats.GetGroupRules(ctx, req.ChatId)
	switch {
	case errors.Is(err, chat.ErrChatNotFound):
		return &paymentrequestpb.GetChatPaymentRequestsResponse{Result: paymentrequestpb.GetChatPaymentRequestsResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting chat rules")
		return nil, status.Error(codes.Internal, "")
	}

	canListen, err := s.access.CanListenWithRules(ctx, req.ChatId, rules, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure determining listener standing")
		return nil, status.Error(codes.Internal, "")
	}
	if !canListen {
		return &paymentrequestpb.GetChatPaymentRequestsResponse{Result: paymentrequestpb.GetChatPaymentRequestsResponse_DENIED}, nil
	}

	now := s.now()
	page, hasMore, err := s.chatPage(ctx, req.ChatId, section, after, limit, now)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure reading payment request page")
		return nil, status.Error(codes.Internal, "")
	}

	resp := &paymentrequestpb.GetChatPaymentRequestsResponse{
		Result:  paymentrequestpb.GetChatPaymentRequestsResponse_OK,
		HasMore: hasMore,
	}
	for _, r := range page {
		resp.PaymentRequests = append(resp.PaymentRequests, r.ToProto(now))
	}
	if hasMore {
		resp.PagingToken = encodePageToken(req.ChatId, section, CursorOf(page[len(page)-1], section))
	}
	return resp, nil
}

// chatPage reads up to limit of chatID's requests in section strictly after
// the cursor, as shown at now, and reports whether more follow. It asks the
// store for one more than it shows, so has_more is exact, and in the open
// section drops each request no longer open at now (see StatusAt) and reads
// on from the last request read until it has that many or the store runs dry.
func (s *Server) chatPage(ctx context.Context, chatID *commonpb.ChatId, section Section, after *PageCursor, limit int, now time.Time) ([]*PaymentRequest, bool, error) {
	var shown []*PaymentRequest
	cursor := after
	for len(shown) <= limit {
		want := limit + 1 - len(shown)
		batch, err := s.requests.GetChatPage(ctx, chatID, section, cursor, want)
		if err != nil {
			return nil, false, err
		}
		for _, r := range batch {
			if section == SectionOpen && !r.IsOpenAt(now) {
				continue
			}
			shown = append(shown, r)
		}
		if len(batch) < want {
			break
		}
		cursor = CursorOf(batch[len(batch)-1], section)
	}

	if len(shown) > limit {
		return shown[:limit], true, nil
	}
	return shown, false, nil
}

// sectionFromProto maps a request's section onto Section.
func sectionFromProto(section paymentrequestpb.GetChatPaymentRequestsRequest_Section) (Section, bool) {
	switch section {
	case paymentrequestpb.GetChatPaymentRequestsRequest_OPEN:
		return SectionOpen, true
	case paymentrequestpb.GetChatPaymentRequestsRequest_RESOLVED:
		return SectionResolved, true
	default:
		return 0, false
	}
}

// encodePageToken serializes a page's resume position, bound to the chat and
// section it pages: the version byte, the section, the chat ID, the cursor's
// sort time as big-endian nanoseconds and the request ID.
func encodePageToken(chatID *commonpb.ChatId, section Section, cursor *PageCursor) *commonpb.PagingToken {
	buf := make([]byte, pageTokenLen)
	buf[0] = pageTokenVersion
	buf[1] = byte(section)
	copy(buf[2:2+groupChatIDSize], chatID.Value)
	binary.BigEndian.PutUint64(buf[2+groupChatIDSize:], uint64(cursor.At.UnixNano()))
	copy(buf[2+groupChatIDSize+8:], cursor.ID)
	return &commonpb.PagingToken{Value: buf}
}

// decodePageToken parses a token minted for chatID and section; a malformed
// token, or one bound to another chat or section, is refused.
func decodePageToken(token *commonpb.PagingToken, chatID *commonpb.ChatId, section Section) (*PageCursor, bool) {
	b := token.GetValue()
	if len(b) != pageTokenLen || b[0] != pageTokenVersion || b[1] != byte(section) {
		return nil, false
	}
	if !bytes.Equal(b[2:2+groupChatIDSize], chatID.GetValue()) {
		return nil, false
	}
	return &PageCursor{
		At: time.Unix(0, int64(binary.BigEndian.Uint64(b[2+groupChatIDSize:]))).UTC(),
		ID: append([]byte(nil), b[2+groupChatIDSize+8:]...),
	}, true
}

// ClosePaymentRequest closes an open request. A request that does not exist is
// NOT_FOUND, and only its creator may close it (DENIED), whatever their
// standing in its chat now. Closing a request that is not open, stored final
// or past its ends_at, is the no-op OK (see Store.Close), answered with the
// request as it is.
func (s *Server) ClosePaymentRequest(ctx context.Context, req *paymentrequestpb.ClosePaymentRequestRequest) (*paymentrequestpb.ClosePaymentRequestResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("payment_request_id", hex.EncodeToString(req.GetPaymentRequestId().GetValue())),
	)

	r, err := s.requests.Get(ctx, req.PaymentRequestId)
	switch {
	case errors.Is(err, ErrNotFound):
		return &paymentrequestpb.ClosePaymentRequestResponse{Result: paymentrequestpb.ClosePaymentRequestResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting payment request")
		return nil, status.Error(codes.Internal, "")
	}
	if !proto.Equal(r.Creator, userID) {
		return &paymentrequestpb.ClosePaymentRequestResponse{Result: paymentrequestpb.ClosePaymentRequestResponse_DENIED}, nil
	}

	now := s.now()
	r, _, err = s.requests.Close(ctx, req.PaymentRequestId, now)
	switch {
	case errors.Is(err, ErrNotFound):
		return &paymentrequestpb.ClosePaymentRequestResponse{Result: paymentrequestpb.ClosePaymentRequestResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure closing payment request")
		return nil, status.Error(codes.Internal, "")
	}

	return &paymentrequestpb.ClosePaymentRequestResponse{
		Result:         paymentrequestpb.ClosePaymentRequestResponse_OK,
		PaymentRequest: r.ToProto(now),
	}, nil
}

// MarkPaid marks a debtor's share paid by hand (PaidShare.Manual). A request
// that does not exist is NOT_FOUND, only its creator may mark a share
// (DENIED), and the request must have a share for the user (NOT_A_SHARE). A
// share already paid, by payment, by hand or from creation, is the no-op OK
// whatever the request's status; an unpaid share of a request that is not
// open is NOT_OPEN (see Store.PayShare). No message is posted.
func (s *Server) MarkPaid(ctx context.Context, req *paymentrequestpb.MarkPaidRequest) (*paymentrequestpb.MarkPaidResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(
		zap.String("user_id", model.UserIDString(userID)),
		zap.String("payment_request_id", hex.EncodeToString(req.GetPaymentRequestId().GetValue())),
		zap.String("debtor_id", model.UserIDString(req.GetUserId())),
	)

	r, err := s.requests.Get(ctx, req.PaymentRequestId)
	switch {
	case errors.Is(err, ErrNotFound):
		return &paymentrequestpb.MarkPaidResponse{Result: paymentrequestpb.MarkPaidResponse_NOT_FOUND}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting payment request")
		return nil, status.Error(codes.Internal, "")
	}
	if !proto.Equal(r.Creator, userID) {
		return &paymentrequestpb.MarkPaidResponse{Result: paymentrequestpb.MarkPaidResponse_DENIED}, nil
	}
	if r.Share(req.UserId) == nil {
		return &paymentrequestpb.MarkPaidResponse{Result: paymentrequestpb.MarkPaidResponse_NOT_A_SHARE}, nil
	}

	now := s.now()
	paid := &paymentrequestpb.PaidShare{
		PaidAt: timestamppb.New(now),
		How:    &paymentrequestpb.PaidShare_Manual_{Manual: &paymentrequestpb.PaidShare_Manual{}},
	}
	r, _, err = s.requests.PayShare(ctx, req.PaymentRequestId, req.UserId, paid, now)
	switch {
	case errors.Is(err, ErrNotFound):
		return &paymentrequestpb.MarkPaidResponse{Result: paymentrequestpb.MarkPaidResponse_NOT_FOUND}, nil
	case errors.Is(err, ErrNotAShare):
		return &paymentrequestpb.MarkPaidResponse{Result: paymentrequestpb.MarkPaidResponse_NOT_A_SHARE}, nil
	case errors.Is(err, ErrNotOpen):
		return &paymentrequestpb.MarkPaidResponse{Result: paymentrequestpb.MarkPaidResponse_NOT_OPEN}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure marking share paid")
		return nil, status.Error(codes.Internal, "")
	}

	return &paymentrequestpb.MarkPaidResponse{
		Result:         paymentrequestpb.MarkPaidResponse_OK,
		PaymentRequest: r.ToProto(now),
	}, nil
}
