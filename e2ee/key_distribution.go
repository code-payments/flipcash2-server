package e2ee

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
	e2eepb "github.com/code-payments/flipcash2-protobuf-api/generated/go/e2ee/v1"

	"github.com/code-payments/flipcash2-server/account"
	"github.com/code-payments/flipcash2-server/auth"
	"github.com/code-payments/flipcash2-server/model"
)

// KeyDistributionServer implements e2ee.v1.KeyDistribution: device
// registration, prekey upload and prekey bundle distribution.
//
// Not built, and recorded here rather than as TODOs: GetPreKeyBundles'
// shared-chat and blocklist refusals (DENIED is never answered), and
// expiry of a device that never opens its mailbox. Each needs a system
// this package does not yet touch.
type KeyDistributionServer struct {
	log      *zap.Logger
	authz    auth.Authorizer
	accounts account.Store
	store    Store
	limits   Limits

	// required are the capabilities every new device must declare
	// (RegisterDeviceResponse.MISSING_REQUIRED_CAPABILITY);
	// DefaultRequiredCapabilities unless an option replaces them.
	required []e2eepb.Device_Capability

	e2eepb.UnimplementedKeyDistributionServer
}

// KeyDistributionOption configures a KeyDistributionServer.
type KeyDistributionOption func(*KeyDistributionServer)

// DefaultRequiredCapabilities are the capabilities a new device must
// declare unless WithRequiredCapabilities says otherwise: the
// post-quantum ratchet, so that every session a peer builds with a device
// registered here can be a triple ratchet. Signal-Server requires the same
// of every newly linked device. Required from the first registration
// rather than added later, since a floor raised once devices exist would
// have to wait out every build below it.
var DefaultRequiredCapabilities = []e2eepb.Device_Capability{e2eepb.Device_SPARSE_POST_QUANTUM_RATCHET}

// WithRequiredCapabilities replaces DefaultRequiredCapabilities: a
// registration that does not declare every one of caps is refused, which
// is how a client build below a floor is kept off the roster. With no
// caps, nothing is required.
func WithRequiredCapabilities(caps ...e2eepb.Device_Capability) KeyDistributionOption {
	return func(s *KeyDistributionServer) { s.required = caps }
}

// WithKeyDistributionLimits replaces DefaultLimits.
func WithKeyDistributionLimits(limits Limits) KeyDistributionOption {
	return func(s *KeyDistributionServer) { s.limits = limits }
}

// NewKeyDistributionServer returns a KeyDistribution service over store.
// accounts decides which users exist (a user with a bound key) for the
// not_found lists.
func NewKeyDistributionServer(log *zap.Logger, authz auth.Authorizer, accounts account.Store, store Store, opts ...KeyDistributionOption) *KeyDistributionServer {
	s := &KeyDistributionServer{
		log:      log,
		authz:    authz,
		accounts: accounts,
		store:    store,
		limits:   DefaultLimits,
		required: DefaultRequiredCapabilities,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *KeyDistributionServer) RegisterDevice(ctx context.Context, req *e2eepb.RegisterDeviceRequest) (*e2eepb.RegisterDeviceResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	// Only a registered account holds devices: a device is a valid recipient
	// from the moment it registers, and an account that has not completed
	// registration is not one anybody should be able to reach. Unregistering
	// is not gated, so an account that loses its registration can still
	// clean up.
	isRegistered, err := s.accounts.IsRegistered(ctx, userID)
	if err != nil {
		log.Warn("Failed to get registration flag", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to get registration flag")
	} else if !isRegistered {
		return &e2eepb.RegisterDeviceResponse{Result: e2eepb.RegisterDeviceResponse_DENIED}, nil
	}

	// The account key is the one the request was authenticated with: the
	// certification chain's root is whatever key the user proves possession of
	// here, and it is served with the device so peers can check it.
	accountKey := req.Auth.GetKeyPair().GetPubKey().GetValue()
	identityKey := req.IdentityKey.GetValue()

	// Everything request-local is checked before the store is touched, so a
	// retry of a refused registration is refused the same way and stores
	// nothing. The idempotent replay is decided by the store, which is the
	// only place that knows whether the first attempt landed.
	if !VerifyIdentityKeySignature(accountKey, identityKey, req.IdentityKeySignature.GetValue()) {
		return &e2eepb.RegisterDeviceResponse{Result: e2eepb.RegisterDeviceResponse_INVALID_SIGNATURE}, nil
	}

	keys := &DeviceKeys{
		SignedPreKey:        SignedPreKeyFromProto(req.SignedPrekey),
		KemLastResortPreKey: KemSignedPreKeyFromProto(req.KemLastResortPrekey),
		OneTimePreKeys:      OneTimePreKeysFromProto(req.OneTimePrekeys),
		KemOneTimePreKeys:   KemSignedPreKeysFromProto(req.KemOneTimePrekeys),
	}
	if !verifyKeys(identityKey, keys.SignedPreKey, keys.KemLastResortPreKey, keys.KemOneTimePreKeys) {
		return &e2eepb.RegisterDeviceResponse{Result: e2eepb.RegisterDeviceResponse_INVALID_SIGNATURE}, nil
	}
	if HasDuplicateKemID(keys.KemLastResortPreKey, keys.KemOneTimePreKeys) || HasDuplicateOneTimeID(keys.OneTimePreKeys) {
		return &e2eepb.RegisterDeviceResponse{Result: e2eepb.RegisterDeviceResponse_DUPLICATE_PREKEY_ID}, nil
	}
	for _, c := range s.required {
		if !slices.Contains(req.Capabilities, c) {
			return &e2eepb.RegisterDeviceResponse{Result: e2eepb.RegisterDeviceResponse_MISSING_REQUIRED_CAPABILITY}, nil
		}
	}

	device := &Device{
		Address:              DeviceAddress{UserID: userID},
		RegistrationID:       req.RegistrationId.GetValue(),
		IdentityKey:          append([]byte(nil), identityKey...),
		IdentityKeySignature: append([]byte(nil), req.IdentityKeySignature.GetValue()...),
		AccountKey:           append([]byte(nil), accountKey...),
		RegisteredAt:         time.Now().UTC(),
		Capabilities:         append([]e2eepb.Device_Capability(nil), req.Capabilities...),
		AppInstall:           req.AppInstall.GetValue(),
	}

	registered, _, err := s.store.RegisterDevice(ctx, req.IdempotencyKey, device, keys)
	switch {
	case errors.Is(err, ErrTooManyDevices):
		return &e2eepb.RegisterDeviceResponse{Result: e2eepb.RegisterDeviceResponse_TOO_MANY_DEVICES}, nil
	case errors.Is(err, ErrDuplicateIdentityKey):
		return &e2eepb.RegisterDeviceResponse{Result: e2eepb.RegisterDeviceResponse_DUPLICATE_IDENTITY_KEY}, nil
	case errors.Is(err, ErrDuplicatePreKeyID):
		return &e2eepb.RegisterDeviceResponse{Result: e2eepb.RegisterDeviceResponse_DUPLICATE_PREKEY_ID}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure registering device")
		return nil, status.Error(codes.Internal, "")
	}

	return &e2eepb.RegisterDeviceResponse{
		Result: e2eepb.RegisterDeviceResponse_OK,
		Device: registered.ToProto(true),
	}, nil
}

func (s *KeyDistributionServer) UnregisterDevice(ctx context.Context, req *e2eepb.UnregisterDeviceRequest) (*e2eepb.UnregisterDeviceResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	address := DeviceAddress{UserID: userID, DeviceID: req.DeviceId.GetValue()}
	log := s.log.With(zap.String("device", address.Key()))

	err = s.store.UnregisterDevice(ctx, address)
	switch {
	case errors.Is(err, ErrDeviceNotFound):
		return &e2eepb.UnregisterDeviceResponse{Result: e2eepb.UnregisterDeviceResponse_UNKNOWN_DEVICE}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure unregistering device")
		return nil, status.Error(codes.Internal, "")
	}

	return &e2eepb.UnregisterDeviceResponse{Result: e2eepb.UnregisterDeviceResponse_OK}, nil
}

func (s *KeyDistributionServer) GetDevices(ctx context.Context, req *e2eepb.GetDevicesRequest) (*e2eepb.GetDevicesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	log := s.log.With(zap.String("user_id", model.UserIDString(userID)))

	userIDs := req.UserIds
	if len(userIDs) == 0 {
		userIDs = []*commonpb.UserId{userID}
	} else if hasDuplicateUser(userIDs) {
		return nil, status.Error(codes.InvalidArgument, "user_ids must not contain duplicates")
	}

	resp := &e2eepb.GetDevicesResponse{Result: e2eepb.GetDevicesResponse_OK}
	for _, target := range userIDs {
		exists, err := s.userExists(ctx, target)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure checking whether user exists")
			return nil, status.Error(codes.Internal, "")
		}
		if !exists {
			resp.NotFound = append(resp.NotFound, target)
			continue
		}

		devices, err := s.store.GetDevices(ctx, target)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting devices")
			return nil, status.Error(codes.Internal, "")
		}

		isSelf := bytes.Equal(target.Value, userID.Value)
		entry := &e2eepb.GetDevicesResponse_UserDevices{UserId: target}
		for _, d := range devices {
			entry.Devices = append(entry.Devices, d.ToProto(isSelf))
		}
		resp.Users = append(resp.Users, entry)
	}
	return resp, nil
}

func (s *KeyDistributionServer) SetKeys(ctx context.Context, req *e2eepb.SetKeysRequest) (*e2eepb.SetKeysResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	address := DeviceAddress{UserID: userID, DeviceID: req.DeviceId.GetValue()}
	log := s.log.With(zap.String("device", address.Key()))

	device, err := s.store.GetDevice(ctx, address)
	switch {
	case errors.Is(err, ErrDeviceNotFound):
		return &e2eepb.SetKeysResponse{Result: e2eepb.SetKeysResponse_UNKNOWN_DEVICE}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting device")
		return nil, status.Error(codes.Internal, "")
	}

	update := KeyUpdate{
		SignedPreKey:        SignedPreKeyFromProto(req.SignedPrekey),
		KemLastResortPreKey: KemSignedPreKeyFromProto(req.KemLastResortPrekey),
		OneTimePreKeys:      OneTimePreKeysFromProto(req.OneTimePrekeys),
		KemOneTimePreKeys:   KemSignedPreKeysFromProto(req.KemOneTimePrekeys),
	}

	// Every signature is against THIS device's identity key, which is what
	// stops a sibling device, authenticated as the same account, from
	// replacing its prekeys. Checked before anything is applied so a refused
	// request applies nothing. Id collisions against the stored last-resort
	// key are the store's to see, and it applies nothing on one either.
	if !verifyKeys(device.IdentityKey, update.SignedPreKey, update.KemLastResortPreKey, update.KemOneTimePreKeys) {
		return &e2eepb.SetKeysResponse{Result: e2eepb.SetKeysResponse_INVALID_SIGNATURE}, nil
	}
	if HasDuplicateKemID(update.KemLastResortPreKey, update.KemOneTimePreKeys) || HasDuplicateOneTimeID(update.OneTimePreKeys) {
		return &e2eepb.SetKeysResponse{Result: e2eepb.SetKeysResponse_DUPLICATE_PREKEY_ID}, nil
	}

	keyStatus, err := s.store.SetKeys(ctx, address, update)
	switch {
	case errors.Is(err, ErrDeviceNotFound):
		return &e2eepb.SetKeysResponse{Result: e2eepb.SetKeysResponse_UNKNOWN_DEVICE}, nil
	case errors.Is(err, ErrDuplicatePreKeyID):
		return &e2eepb.SetKeysResponse{Result: e2eepb.SetKeysResponse_DUPLICATE_PREKEY_ID}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure setting keys")
		return nil, status.Error(codes.Internal, "")
	}

	return &e2eepb.SetKeysResponse{
		Result:            e2eepb.SetKeysResponse_OK,
		Counts:            keyStatus.Counts.ToProto(),
		RepeatedUseDigest: RepeatedUseDigest(device.IdentityKey, keyStatus.SignedPreKey, keyStatus.KemLastResortPreKey),
	}, nil
}

func (s *KeyDistributionServer) GetPreKeyCounts(ctx context.Context, req *e2eepb.GetPreKeyCountsRequest) (*e2eepb.GetPreKeyCountsResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	address := DeviceAddress{UserID: userID, DeviceID: req.DeviceId.GetValue()}
	log := s.log.With(zap.String("device", address.Key()))

	keyStatus, err := s.store.GetKeyStatus(ctx, address)
	switch {
	case errors.Is(err, ErrDeviceNotFound):
		return &e2eepb.GetPreKeyCountsResponse{Result: e2eepb.GetPreKeyCountsResponse_UNKNOWN_DEVICE}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure getting key status")
		return nil, status.Error(codes.Internal, "")
	}

	// The digest rides on the read the client already makes on every
	// foreground, in place of Signal's separate keys/check call.
	return &e2eepb.GetPreKeyCountsResponse{
		Result:            e2eepb.GetPreKeyCountsResponse_OK,
		Counts:            keyStatus.Counts.ToProto(),
		RepeatedUseDigest: RepeatedUseDigest(keyStatus.Device.IdentityKey, keyStatus.SignedPreKey, keyStatus.KemLastResortPreKey),
	}, nil
}

func (s *KeyDistributionServer) GetPreKeyBundles(ctx context.Context, req *e2eepb.GetPreKeyBundlesRequest) (*e2eepb.GetPreKeyBundlesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	callerDevice := DeviceAddress{UserID: userID, DeviceID: req.CallerDeviceId.GetValue()}
	log := s.log.With(zap.String("device", callerDevice.Key()))

	if hasDuplicateUser(req.UserIds) {
		return nil, status.Error(codes.InvalidArgument, "user_ids must not contain duplicates")
	}
	if req.DeviceId != nil && len(req.UserIds) != 1 {
		return nil, status.Error(codes.InvalidArgument, "device_id names one device of one user")
	}

	// The caller's device must exist: it is the one that will hold the
	// sessions, and a self-fetch excludes it.
	if _, err := s.store.GetDevice(ctx, callerDevice); errors.Is(err, ErrDeviceNotFound) {
		return &e2eepb.GetPreKeyBundlesResponse{Result: e2eepb.GetPreKeyBundlesResponse_UNKNOWN_DEVICE}, nil
	} else if err != nil {
		log.With(zap.Error(err)).Warn("Failure getting caller device")
		return nil, status.Error(codes.Internal, "")
	}

	// Existence and the device sets are resolved before any prekey is
	// consumed, so a request refused for its shape spends nothing. Bundles
	// are then taken one device at a time; a failure midway has consumed the
	// earlier devices' prekeys, which is a loss of forward-secrecy material
	// but not of correctness (the caller retries and gets fresh ones).
	resp := &e2eepb.GetPreKeyBundlesResponse{Result: e2eepb.GetPreKeyBundlesResponse_OK}
	var targets []*Device
	for _, target := range req.UserIds {
		exists, err := s.userExists(ctx, target)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure checking whether user exists")
			return nil, status.Error(codes.Internal, "")
		}
		if !exists {
			resp.NotFound = append(resp.NotFound, target)
			continue
		}

		if req.DeviceId != nil {
			address := DeviceAddress{UserID: target, DeviceID: req.DeviceId.GetValue()}
			if address.Key() == callerDevice.Key() {
				// A device holds no session with itself; the unset path
				// excludes it, and naming it outright is a request for
				// nothing that would still spend its prekeys.
				return nil, status.Error(codes.InvalidArgument, "a device does not fetch its own bundle")
			}
			d, err := s.store.GetDevice(ctx, address)
			if errors.Is(err, ErrDeviceNotFound) {
				return &e2eepb.GetPreKeyBundlesResponse{Result: e2eepb.GetPreKeyBundlesResponse_NOT_FOUND}, nil
			} else if err != nil {
				log.With(zap.Error(err)).Warn("Failure getting device")
				return nil, status.Error(codes.Internal, "")
			}
			targets = append(targets, d)
			continue
		}

		devices, err := s.store.GetDevices(ctx, target)
		if err != nil {
			log.With(zap.Error(err)).Warn("Failure getting devices")
			return nil, status.Error(codes.Internal, "")
		}
		for _, d := range devices {
			if d.Address.Key() == callerDevice.Key() {
				continue
			}
			targets = append(targets, d)
		}
	}

	// The limits, before any prekey is spent, so a refused call consumes
	// nothing: the caller's budget across every target first, then each
	// target's. The limiter fails closed here, as Signal's does for
	// fetches, since a fetch is what an attacker drains prekeys with.
	retry, err := s.limitBundles(ctx, callerDevice, targets)
	if err != nil {
		log.With(zap.Error(err)).Warn("Failure taking bundle tokens")
		return nil, status.Error(codes.Internal, "")
	}
	if retry > 0 {
		return &e2eepb.GetPreKeyBundlesResponse{
			Result:     e2eepb.GetPreKeyBundlesResponse_RATE_LIMITED,
			RetryAfter: durationpb.New(retry),
		}, nil
	}

	for _, target := range targets {
		bundle, err := s.store.TakePreKeyBundle(ctx, target.Address)
		if errors.Is(err, ErrDeviceNotFound) {
			// Unregistered between the listing and the take: not a device
			// anymore, so not a bundle. A device named outright has already
			// been answered NOT_FOUND above.
			continue
		} else if err != nil {
			log.With(zap.Error(err)).Warn("Failure taking prekey bundle")
			return nil, status.Error(codes.Internal, "")
		}
		resp.Bundles = append(resp.Bundles, bundle.ToProto())
	}
	return resp, nil
}

// limitBundles takes the tokens a fetch of targets costs: len(targets)
// from the caller's bucket, then one from each (caller, target,
// registration ID) bucket, stopping at the first that refuses. The wait
// returned is that bucket's; 0 when every take landed.
func (s *KeyDistributionServer) limitBundles(ctx context.Context, caller DeviceAddress, targets []*Device) (time.Duration, error) {
	if len(targets) == 0 {
		return 0, nil
	}
	now := time.Now()
	retry, err := s.store.TakeTokens(ctx, limitBundleCaller, deviceKeyString(caller), int64(len(targets)), s.limits.BundlesPerCaller, now)
	if err != nil || retry > 0 {
		return retry, err
	}
	for _, target := range targets {
		retry, err := s.store.TakeTokens(ctx, limitBundleTarget, bundleTargetKey(caller, target), 1, s.limits.BundlesPerTarget, now)
		if err != nil || retry > 0 {
			return retry, err
		}
	}
	return 0, nil
}

// userExists reports whether userID belongs to a known account: one with at
// least one bound public key, as the blocklist and resolver decide it.
func (s *KeyDistributionServer) userExists(ctx context.Context, userID *commonpb.UserId) (bool, error) {
	pubKeys, err := s.accounts.GetPubKeys(ctx, userID)
	if err != nil {
		return false, err
	}
	return len(pubKeys) > 0, nil
}

// verifyKeys checks every supplied signed prekey against identityKey. A nil
// part is not supplied and passes.
func verifyKeys(identityKey []byte, signed *SignedPreKey, lastResort *KemSignedPreKey, kemOneTime []*KemSignedPreKey) bool {
	if signed != nil && !VerifyPreKeySignature(identityKey, signed.PublicKey, signed.Signature) {
		return false
	}
	if lastResort != nil && !VerifyPreKeySignature(identityKey, lastResort.PublicKey, lastResort.Signature) {
		return false
	}
	for _, k := range kemOneTime {
		if !VerifyPreKeySignature(identityKey, k.PublicKey, k.Signature) {
			return false
		}
	}
	return true
}

func hasDuplicateUser(userIDs []*commonpb.UserId) bool {
	seen := make(map[string]struct{}, len(userIDs))
	for _, u := range userIDs {
		if _, dup := seen[string(u.GetValue())]; dup {
			return true
		}
		seen[string(u.GetValue())] = struct{}{}
	}
	return false
}

func (s *KeyDistributionServer) SetCapabilities(ctx context.Context, req *e2eepb.SetCapabilitiesRequest) (*e2eepb.SetCapabilitiesResponse, error) {
	userID, err := s.authz.Authorize(ctx, req, &req.Auth)
	if err != nil {
		return nil, err
	}

	address := DeviceAddress{UserID: userID, DeviceID: req.DeviceId.GetValue()}
	log := s.log.With(zap.String("device", address.Key()))

	// Additive only, so nothing here can strip a capability the server
	// requires of new devices, and the store decides whether anything
	// changes.
	device, err := s.store.AddCapabilities(ctx, address, req.Capabilities)
	switch {
	case errors.Is(err, ErrDeviceNotFound):
		return &e2eepb.SetCapabilitiesResponse{Result: e2eepb.SetCapabilitiesResponse_UNKNOWN_DEVICE}, nil
	case err != nil:
		log.With(zap.Error(err)).Warn("Failure adding capabilities")
		return nil, status.Error(codes.Internal, "")
	}

	return &e2eepb.SetCapabilitiesResponse{
		Result: e2eepb.SetCapabilitiesResponse_OK,
		Device: device.ToProto(true),
	}, nil
}
