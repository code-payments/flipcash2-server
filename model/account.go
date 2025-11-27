package model

import (
	"crypto/ed25519"
	"fmt"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"
)

func MustGenerateUserID() *commonpb.UserId {
	id, err := GenerateUserId()
	if err != nil {
		panic(fmt.Sprintf("failed to generate user id: %v", err))
	}

	return id
}

func GenerateUserId() (*commonpb.UserId, error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}

	return &commonpb.UserId{Value: id[:]}, nil
}

func UserIDString(userID *commonpb.UserId) string {
	if userID == nil {
		return "<nil>"
	}
	id, err := uuid.FromBytes(userID.GetValue())
	if err != nil {
		return fmt.Sprintf("<invalid: %v>", err)
	}
	return id.String()
}

type KeyPair struct {
	priv ed25519.PrivateKey
}

func GenerateKeyPair() (KeyPair, error) {
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return KeyPair{}, err
	}

	return KeyPair{priv: priv}, nil
}

func MustGenerateKeyPair() KeyPair {
	key, err := GenerateKeyPair()
	if err != nil {
		panic(fmt.Sprintf("failed to generate key pair: %v", err))
	}

	return key
}

func (k KeyPair) Proto() *commonpb.PublicKey {
	return &commonpb.PublicKey{
		Value: k.priv.Public().(ed25519.PublicKey),
	}
}

func (k KeyPair) Public() ed25519.PublicKey {
	return k.priv.Public().(ed25519.PublicKey)
}

func (k KeyPair) Private() ed25519.PrivateKey {
	return k.priv
}

func (k KeyPair) Sign(m proto.Message, target **commonpb.Signature) error {
	b, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	sig := ed25519.Sign(k.priv, b)
	*target = &commonpb.Signature{Value: sig[:]}
	return nil
}

func (k KeyPair) Auth(m proto.Message, target **commonpb.Auth) error {
	auth := &commonpb.Auth{
		Kind: &commonpb.Auth_KeyPair_{
			KeyPair: &commonpb.Auth_KeyPair{
				PubKey: k.Proto(),
			},
		},
	}

	*target = nil
	if err := k.Sign(m, &auth.GetKeyPair().Signature); err != nil {
		return err
	}
	*target = auth
	return nil
}
