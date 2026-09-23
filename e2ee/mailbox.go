package e2ee

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	e2eepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/e2ee/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
)

const (
	// maxClientTSAhead is how far in the future a send's client_ts may be.
	// There is no bound behind: the value is the sender's correlation
	// handle, not the server's clock, nothing on the server keys on it, and
	// a device whose clock is wrong must still be able to send, so the
	// server trusts it as Signal-Server does. A value years ahead is
	// garbage, not skew.
	maxClientTSAhead = 365 * 24 * time.Hour

	// envelopeRetention is how long an unacknowledged envelope is held
	// before the store may forget it (Envelope.ExpiresAt). Signal holds
	// undelivered messages for 30 to 46 days.
	envelopeRetention = 30 * 24 * time.Hour

	// ephemeralWindow is how long an `online` send is held: long enough
	// for a device that is streaming or draining right now, and no longer,
	// as Signal drops an ephemeral message older than 10 seconds.
	ephemeralWindow = 10 * time.Second

	// maxPlaintextContentSize bounds a PLAINTEXT_CONTENT envelope: a
	// DecryptionErrorMessage is under 100 bytes plus padding, and anything
	// larger, unencrypted and injectable by any member, is refused rather
	// than relayed. A server rule, since validation cannot vary by type.
	maxPlaintextContentSize = 1024

	// maxGetEnvelopesPageSize bounds a GetEnvelopes page. It matches the
	// max_items on EnvelopeBatch.envelopes.
	maxGetEnvelopesPageSize = 100

	// maxGetEnvelopesPageBytes bounds a GetEnvelopes page by content: a
	// full page of 96 KiB envelopes would be 9.6 MB, past gRPC's 4 MiB
	// message limit, and a mailbox whose next hundred envelopes were large
	// could never be drained. Half the message limit, counted on content
	// alone (a PREKEY_MESSAGE's source_device stamp and a receipt's
	// acknowledgements are a few KB at most), so the rest of the response
	// always fits. Signal-Server sends one envelope per WebSocket request
	// and has no page to bound; a client here reads more pages instead.
	maxGetEnvelopesPageBytes = 2 << 20
)

// MailboxServer implements e2ee.v1.Mailbox: the relay that fills, drains and
// empties per-device mailboxes.
//
// Standalone for now, which shows in three places. A chat is a DM whose
// membership is its ID (DeriveDmChatID); a group chat ID is NOT_FOUND and a
// sender-key send is refused, and no chat rule is evaluated, so
// RULES_NOT_SATISFIED is never answered. There is no live delivery and no
// push: an `online` send is stored for ephemeralWindow, marked ephemeral,
// and reaches only a device that drains within it; every other send waits
// for the recipient's next drain. And delivery receipts are issued from the acknowledging call
// after the acknowledgement lands, not in the same write, so a failure in
// between loses the receipt (logged), never the acknowledgement; they are
// folded within the call (one receipt per sender device naming every send
// the call acknowledged) and not yet across calls. Each of these is the
// integration with chat/, event/ and push/ that is next.
type MailboxServer struct {
	log      *zap.Logger
	authz    auth.Authorizer
	accounts account.Store
	store    Store
	limits   Limits
	lastSeen lastSeenMemo

	// pageBytes is the content budget of a GetEnvelopes page
	// (maxGetEnvelopesPageBytes unless an option lowers it for a test).
	pageBytes int64

	e2eepb.UnimplementedMailboxServer
}

// MailboxOption configures a MailboxServer.
type MailboxOption func(*MailboxServer)

// WithMailboxLimits replaces DefaultLimits.
func WithMailboxLimits(limits Limits) MailboxOption {
	return func(s *MailboxServer) { s.limits = limits }
}

// WithGetEnvelopesPageBytes replaces the content budget of a GetEnvelopes
// page, for tests that want to cross it with small envelopes.
func WithGetEnvelopesPageBytes(n int64) MailboxOption {
	return func(s *MailboxServer) { s.pageBytes = n }
}

// lastSeenMemo remembers, per device, the last day this process recorded
// as the day the device was last seen, so a device active on every push
// costs one write per day per server rather than one per call. A device's
// day is its own (LastSeenDay: UTC days shifted by a per-user offset), so
// the memo holds a day per device rather than one day for all, and prunes
// what is two days old when the calendar day turns over. last_seen is a
// coarse liveness signal (for the device expiry that is not built yet, and
// for the user's own device list), not presence, which is why a day is
// enough.
type lastSeenMemo struct {
	mu   sync.Mutex
	day  time.Time // the UTC day the memo last pruned on
	seen map[string]time.Time
}

// due reports whether the device has not yet been recorded for day, its
// own day as of now.
func (m *lastSeenMemo) due(device string, day, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if today := now.UTC().Truncate(24 * time.Hour); today.After(m.day) {
		// A device's day trails the calendar's by less than a day, so an
		// entry two days behind is one no device will ever match again.
		m.day = today
		for k, d := range m.seen {
			if d.Before(today.Add(-24 * time.Hour)) {
				delete(m.seen, k)
			}
		}
	}
	return m.seen[device].Before(day)
}

// mark records that the device was recorded for day.
func (m *lastSeenMemo) mark(device string, day time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = make(map[string]time.Time)
	}
	if day.After(m.seen[device]) {
		m.seen[device] = day
	}
}

// touchLastSeen records that the device was active now, at day
// granularity and at most once per device per day from this process (see
// lastSeenMemo); the store writes only when the day it holds is older, so
// a fleet of servers costs one write each per device per day and no read
// precedes any call. Every mailbox call counts (a send, a drain, an
// acknowledgement), as every authenticated request does on Signal, so a
// device that streams live and only ever sends is still seen. Best
// effort: a call that cannot note itself still succeeds.
func (s *MailboxServer) touchLastSeen(ctx context.Context, log *zap.Logger, address DeviceAddress, now time.Time) {
	day := LastSeenDay(address.UserID, now)
	if !s.lastSeen.due(address.Key(), day, now) {
		return
	}
	if err := s.store.TouchLastSeen(ctx, address, day); err != nil {
		log.With(zap.Error(err)).Warn("Failure recording device activity")
		return
	}
	s.lastSeen.mark(address.Key(), day)
}

// NewMailboxServer returns a Mailbox service over store. accounts decides
// which users exist: a DM's peer must.
func NewMailboxServer(log *zap.Logger, authz auth.Authorizer, accounts account.Store, store Store, opts ...MailboxOption) *MailboxServer {
	s := &MailboxServer{
		log:       log,
		authz:     authz,
		accounts:  accounts,
		store:     store,
		limits:    DefaultLimits,
		pageBytes: maxGetEnvelopesPageBytes,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *MailboxServer) SendEnvelopes(ctx context.Context, req *e2eepb.SendEnvelopesRequest) (*e2eepb.SendEnvelopesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	sender := DeviceAddress{UserID: userID, DeviceID: req.DeviceId.GetValue()}
	log := s.log.With(
		zap.String("device", sender.Key()),
		zap.String("chat_id", model.ChatIDString(req.ChatId)),
	)

	// Only DMs exist here. A group's ID names a chat this package has no
	// record of.
	if !IsDmChatID(req.ChatId) {
		return &e2eepb.SendEnvelopesResponse{Result: e2eepb.SendEnvelopesResponse_NOT_FOUND}, nil
	}

	// The sender's own devices: the sending one must exist, and the others
	// are held to be named (see the sibling rule below).
	senderDevices, err := s.store.GetDevices(ctx, userID)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting sender devices")
		return nil, status.Error(codes.Internal, "")
	}
	var senderDevice *Device
	for _, d := range senderDevices {
		if d.Address.DeviceID == sender.DeviceID {
			senderDevice = d
		}
	}
	if senderDevice == nil {
		return &e2eepb.SendEnvelopesResponse{Result: e2eepb.SendEnvelopesResponse_UNKNOWN_DEVICE}, nil
	}

	now := time.Now().UTC()
	clientTS := req.ClientTs.AsTime()
	if clientTS.Nanosecond()%int(time.Millisecond) != 0 || clientTS.After(now.Add(maxClientTSAhead)) {
		return &e2eepb.SendEnvelopesResponse{Result: e2eepb.SendEnvelopesResponse_INVALID_TIMESTAMP}, nil
	}

	// A DM is always pairwise: a sender key is a group's, and there are no
	// groups.
	pairwise := req.GetPairwise()
	if pairwise == nil {
		return nil, status.Error(codes.InvalidArgument, "a DM is sent pairwise; sender keys are for groups, which are not supported")
	}

	// Shape the recipients before any read: one entry per device, none of
	// them the sender, plaintext within its bound.
	named := make(map[string]struct{}, len(pairwise.Envelopes))
	byUser := make(map[string]map[uint32]uint32) // user -> device id -> registration id named
	var users []*commonpb.UserId
	for _, pe := range pairwise.Envelopes {
		recipient := AddressFromProto(pe.Recipient)
		if recipient.Key() == sender.Key() {
			return nil, status.Error(codes.InvalidArgument, "a device never addresses itself")
		}
		if _, dup := named[recipient.Key()]; dup {
			return nil, status.Error(codes.InvalidArgument, "at most one envelope per recipient device")
		}
		named[recipient.Key()] = struct{}{}
		if pe.Type == e2eepb.Envelope_PLAINTEXT_CONTENT && len(pe.Content) > maxPlaintextContentSize {
			return nil, status.Error(codes.InvalidArgument, "plaintext content exceeds 1 KiB")
		}

		userKey := string(recipient.UserID.Value)
		if _, ok := byUser[userKey]; !ok {
			byUser[userKey] = make(map[uint32]uint32)
			users = append(users, recipient.UserID)
		}
		byUser[userKey][recipient.DeviceID] = pe.RegistrationId.GetValue()
	}

	// Membership: the DM is between the sender and at most one peer, and its
	// ID commits to the pair. A send naming only the sender's own devices
	// has no peer to check the ID against and is taken on the sender's word;
	// its siblings read it as their own user's message either way.
	var peer *commonpb.UserId
	for _, u := range users {
		if bytes.Equal(u.Value, userID.Value) {
			continue
		}
		if peer != nil {
			return &e2eepb.SendEnvelopesResponse{Result: e2eepb.SendEnvelopesResponse_INVALID_RECIPIENT}, nil
		}
		peer = u
	}
	if peer != nil {
		if !bytes.Equal(DeriveDmChatID(userID, peer).Value, req.ChatId.Value) {
			return &e2eepb.SendEnvelopesResponse{Result: e2eepb.SendEnvelopesResponse_INVALID_RECIPIENT}, nil
		}
		exists, err := s.userExists(ctx, peer)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure checking whether peer exists")
			return nil, status.Error(codes.Internal, "")
		}
		if !exists {
			return &e2eepb.SendEnvelopesResponse{Result: e2eepb.SendEnvelopesResponse_INVALID_RECIPIENT}, nil
		}
	}

	// The sender's own user is held to the rule whether or not the call
	// names it: a sender with other devices names every one of them, or is
	// told which it omitted, so a sibling is never left with a
	// conversation missing its own side (Signal's sync-message rule). A
	// sender with no other devices has nothing to omit and passes.
	if _, named := byUser[string(userID.Value)]; !named {
		byUser[string(userID.Value)] = make(map[uint32]uint32)
		users = append(users, userID)
	}

	// Sesame's rule: for every recipient user named, the devices named must
	// be exactly the user's current devices, under their current
	// registration IDs, the sending device itself excepted.
	var mismatches []*e2eepb.DeviceMismatch
	for _, u := range users {
		current := senderDevices
		if !bytes.Equal(u.Value, userID.Value) {
			if current, err = s.store.GetDevices(ctx, u); err != nil {
				log.With(zap.Error(err)).Warn("Failure getting recipient devices")
				return nil, status.Error(codes.Internal, "")
			}
		}
		if m := deviceMismatch(u, sender, current, byUser[string(u.Value)]); m != nil {
			mismatches = append(mismatches, m)
		}
	}
	if len(mismatches) > 0 {
		return &e2eepb.SendEnvelopesResponse{
			Result:            e2eepb.SendEnvelopesResponse_MISMATCHED_DEVICES,
			MismatchedDevices: mismatches,
		}, nil
	}

	// The limits, after every check and before the write, so a refused
	// call has stored nothing: sends per (sender, peer) and bytes per peer,
	// the peer only, since a sender's own devices are its own. The limiter
	// fails open here, as Signal's does for sends: a limiter outage must
	// not stop messaging, and a refused send is answered with how long to
	// wait.
	if peer != nil {
		var inbound int64
		for _, pe := range pairwise.Envelopes {
			if bytes.Equal(pe.Recipient.GetUserId().GetValue(), peer.Value) {
				inbound += int64(len(pe.Content))
			}
		}
		if retry := s.limitSend(ctx, log, userID, peer, inbound, now); retry > 0 {
			return &e2eepb.SendEnvelopesResponse{
				Result:     e2eepb.SendEnvelopesResponse_RATE_LIMITED,
				RetryAfter: durationpb.New(retry),
			}, nil
		}
	}

	// An online send is held for the ephemeral window and lapses unread
	// for a device that does not drain within it; everything else is held
	// for the retention.
	expiresAt := now.Add(envelopeRetention)
	if req.Online {
		expiresAt = now.Add(ephemeralWindow)
	}
	envelopes := make([]*Envelope, 0, len(pairwise.Envelopes))
	for _, pe := range pairwise.Envelopes {
		recipient := AddressFromProto(pe.Recipient)
		source := sender.Clone()
		ts := clientTS
		e := &Envelope{
			Recipient:   recipient,
			ChatID:      cloneChatID(req.ChatId),
			Source:      &source,
			Type:        pe.Type,
			Content:     append([]byte(nil), pe.Content...),
			ClientTS:    &ts,
			ServerTS:    now,
			LowPriority: req.LowPriority,
			ContentHint: req.ContentHint,
			Ephemeral:   req.Online,
			ExpiresAt:   expiresAt,
			// A receipt is earned by a ciphertext acknowledged by another
			// user's device: never by plaintext, never by a sibling, never
			// by an online send, never when the sender declined it.
			WantsDeliveryReceipt: !req.NoDeliveryReceipt && !req.Online &&
				pe.Type != e2eepb.Envelope_PLAINTEXT_CONTENT &&
				!bytes.Equal(recipient.UserID.Value, userID.Value),
		}
		if pe.Type == e2eepb.Envelope_PREKEY_MESSAGE {
			// The stamp is the record a peer may see: never last_seen,
			// never the app install.
			e.SourceDevice = senderDevice.Clone()
			e.SourceDevice.LastSeen = nil
			e.SourceDevice.AppInstall = ""
		}
		envelopes = append(envelopes, e)
	}

	if _, err := s.store.Deliver(ctx, envelopes); errors.Is(err, ErrDeviceNotFound) {
		// A recipient went away between the check and the write. The
		// retry sees it as an extra device.
		return nil, status.Error(codes.Aborted, "a recipient's devices changed; retry")
	} else if err != nil {
		log.With(zap.Error(err)).Warn("Failure delivering envelopes")
		return nil, status.Error(codes.Internal, "")
	}

	s.touchLastSeen(ctx, log, sender, now)

	return &e2eepb.SendEnvelopesResponse{
		Result:   e2eepb.SendEnvelopesResponse_OK,
		ServerTs: timestamppb.New(now),
	}, nil
}

func (s *MailboxServer) GetEnvelopes(ctx context.Context, req *e2eepb.GetEnvelopesRequest) (*e2eepb.GetEnvelopesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	address := DeviceAddress{UserID: userID, DeviceID: req.DeviceId.GetValue()}
	log := s.log.With(zap.String("device", address.Key()))

	limit := maxGetEnvelopesPageSize
	if pageSize := req.GetQueryOptions().GetPageSize(); pageSize > 0 && int(pageSize) < limit {
		limit = int(pageSize)
	}

	// One past the limit tells whether more remain; the byte budget cuts a
	// page of large envelopes short of that, and a page cut by bytes is
	// answered has_more with the envelope that crossed the budget held
	// back for the next page (a single envelope over the budget is served
	// alone). When the crossing envelope was the mailbox's last, has_more
	// is answered once too often and the next page is empty: one spare
	// call, never a lost envelope.
	envelopes, err := s.store.GetEnvelopes(ctx, address, req.AfterSequence, limit+1, s.pageBytes)
	switch {
	case errors.Is(err, ErrDeviceNotFound):
		return &e2eepb.GetEnvelopesResponse{Result: e2eepb.GetEnvelopesResponse_UNKNOWN_DEVICE}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting envelopes")
		return nil, status.Error(codes.Internal, "")
	}

	s.touchLastSeen(ctx, log, address, time.Now())

	resp := &e2eepb.GetEnvelopesResponse{Result: e2eepb.GetEnvelopesResponse_OK}
	if len(envelopes) > limit {
		resp.HasMore = true
		envelopes = envelopes[:limit]
	}
	var bytes int64
	for _, e := range envelopes {
		bytes += int64(len(e.Content))
	}
	if s.pageBytes > 0 && bytes > s.pageBytes {
		resp.HasMore = true
		if len(envelopes) > 1 {
			envelopes = envelopes[:len(envelopes)-1]
		}
	}
	if len(envelopes) > 0 {
		resp.Envelopes = &e2eepb.EnvelopeBatch{}
		for _, e := range envelopes {
			resp.Envelopes.Envelopes = append(resp.Envelopes.Envelopes, e.ToProto())
		}
	}
	return resp, nil
}

func (s *MailboxServer) AckEnvelopes(ctx context.Context, req *e2eepb.AckEnvelopesRequest) (*e2eepb.AckEnvelopesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	address := DeviceAddress{UserID: userID, DeviceID: req.DeviceId.GetValue()}
	log := s.log.With(zap.String("device", address.Key()))

	ids := make([][]byte, 0, len(req.EnvelopeIds))
	for _, id := range req.EnvelopeIds {
		ids = append(ids, id.GetValue())
	}

	acked, err := s.store.AckEnvelopes(ctx, address, ids)
	switch {
	case errors.Is(err, ErrDeviceNotFound):
		return &e2eepb.AckEnvelopesResponse{Result: e2eepb.AckEnvelopesResponse_UNKNOWN_DEVICE}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure acknowledging envelopes")
		return nil, status.Error(codes.Internal, "")
	}

	now := time.Now().UTC()
	s.touchLastSeen(ctx, log, address, now)

	receipts := s.deliveryReceipts(ctx, address, acked, now)
	if len(receipts) > 0 {
		if _, err := s.store.Deliver(ctx, receipts); err != nil {
			// The acknowledgement has landed and the envelopes are gone; a
			// retry would find nothing to receipt. Lost, and said so.
			log.With(zap.Error(err), zap.Int("receipts", len(receipts))).Warn("Failure delivering receipts for acknowledged envelopes")
		}
	}

	return &e2eepb.AckEnvelopesResponse{Result: e2eepb.AckEnvelopesResponse_OK}, nil
}

// deliveryReceipts builds the SERVER_DELIVERY_RECEIPT envelopes that
// acknowledger's acknowledgement of acked earns: one per (source device,
// chat) with at least one send that wanted a receipt, naming every such
// send's client_ts, delivered to every current device of the source user.
// Folding is within the call; acknowledgements from separate calls are
// separate receipts, which clients treat as a monotonic set. A source user
// with no devices left earns nothing.
func (s *MailboxServer) deliveryReceipts(ctx context.Context, acknowledger DeviceAddress, acked []*Envelope, now time.Time) []*Envelope {
	type sendKey struct {
		source string
		chat   string
	}
	// One receipt per key, in first-seen order so the output is stable.
	type pending struct {
		source   DeviceAddress
		chatID   *commonpb.ChatId
		clientTS []time.Time
	}
	var order []sendKey
	byKey := make(map[sendKey]*pending)
	for _, e := range acked {
		if !e.WantsDeliveryReceipt || e.Source == nil || e.ClientTS == nil {
			continue
		}
		key := sendKey{source: e.Source.Key(), chat: string(e.ChatID.GetValue())}
		p, ok := byKey[key]
		if !ok {
			p = &pending{source: e.Source.Clone(), chatID: cloneChatID(e.ChatID)}
			byKey[key] = p
			order = append(order, key)
		}
		if !containsTime(p.clientTS, *e.ClientTS) {
			p.clientTS = append(p.clientTS, *e.ClientTS)
		}
	}

	devicesOf := make(map[string][]*Device)
	var receipts []*Envelope
	for _, key := range order {
		p := byKey[key]
		userKey := string(p.source.UserID.Value)
		devices, ok := devicesOf[userKey]
		if !ok {
			var err error
			devices, err = s.store.GetDevices(ctx, p.source.UserID)
			if err != nil {
				s.log.With(zap.Error(err), zap.String("device", p.source.Key())).Warn("Failure getting source devices for receipt")
				continue
			}
			devicesOf[userKey] = devices
		}
		for _, d := range devices {
			receipts = append(receipts, &Envelope{
				Recipient: d.Address.Clone(),
				ChatID:    cloneChatID(p.chatID),
				Type:      e2eepb.Envelope_SERVER_DELIVERY_RECEIPT,
				ServerTS:  now,
				ExpiresAt: now.Add(envelopeRetention),
				// Nothing to wake for: the client applies it when it next
				// drains.
				LowPriority: true,
				DeliveryReceipt: &DeliveryReceipt{
					Acknowledgements: []Acknowledgement{{
						Recipient: acknowledger.Clone(),
						ClientTS:  append([]time.Time(nil), p.clientTS...),
					}},
				},
			})
		}
	}
	return receipts
}

// limitSend takes one token from the sender-to-peer bucket and inbound
// bytes from the peer's, and returns the longer wait when either refuses.
// A limiter error is logged and ignored: open, never closed.
func (s *MailboxServer) limitSend(ctx context.Context, log *zap.Logger, sender, peer *commonpb.UserId, inbound int64, now time.Time) time.Duration {
	retryPair, err := s.store.TakeTokens(ctx, limitSendPair, sendPairKey(sender, peer), 1, s.limits.SendsPerPair, now)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure taking send tokens; allowing the send")
		retryPair = 0
	}
	retryBytes, err := s.store.TakeTokens(ctx, limitInbound, model.UserIDString(peer), inbound, s.limits.InboundBytes, now)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure taking inbound tokens; allowing the send")
		retryBytes = 0
	}
	return max(retryPair, retryBytes)
}

func containsTime(ts []time.Time, t time.Time) bool {
	for _, x := range ts {
		if x.Equal(t) {
			return true
		}
	}
	return false
}

// deviceMismatch compares the devices a send named for user against the
// user's current devices, per Sesame, and returns the disagreement, or nil
// when they match exactly. When user is the sender's own, sender itself is
// neither expected nor reported.
func deviceMismatch(user *commonpb.UserId, sender DeviceAddress, current []*Device, named map[uint32]uint32) *e2eepb.DeviceMismatch {
	m := &e2eepb.DeviceMismatch{UserId: cloneUserID(user)}

	currentByID := make(map[uint32]*Device, len(current))
	for _, d := range current {
		if d.Address.Key() == sender.Key() {
			continue
		}
		currentByID[d.Address.DeviceID] = d
		regID, ok := named[d.Address.DeviceID]
		switch {
		case !ok:
			m.MissingDevices = append(m.MissingDevices, &e2eepb.DeviceId{Value: d.Address.DeviceID})
		case regID != d.RegistrationID:
			m.StaleDevices = append(m.StaleDevices, &e2eepb.DeviceId{Value: d.Address.DeviceID})
		}
	}
	for id := range named {
		if _, ok := currentByID[id]; !ok {
			m.ExtraDevices = append(m.ExtraDevices, &e2eepb.DeviceId{Value: id})
		}
	}
	sort.Slice(m.ExtraDevices, func(i, j int) bool {
		return m.ExtraDevices[i].Value < m.ExtraDevices[j].Value
	})

	if len(m.MissingDevices) == 0 && len(m.ExtraDevices) == 0 && len(m.StaleDevices) == 0 {
		return nil
	}
	return m
}

func (s *MailboxServer) userExists(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	pubKeys, err := s.accounts.GetPubKeys(ctx, userID)
	if err != nil {
		return false, err
	}
	return len(pubKeys) > 0, nil
}
