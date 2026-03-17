package memory

import (
	"context"
	"sync"

	commonpb "github.com/code-payments/flipcash2-protobuf-api/generated/go/common/v1"

	"github.com/code-payments/flipcash2-server/settings"
)

type memory struct {
	sync.Mutex

	// maps userID to settings
	prefs map[string]*settings.Settings
}

func NewInMemory() settings.Store {
	return &memory{
		prefs: make(map[string]*settings.Settings),
	}
}

func (m *memory) GetSettings(_ context.Context, userID *commonpb.UserId) (*settings.Settings, error) {
	m.Lock()
	defer m.Unlock()

	key := string(userID.Value)

	p, ok := m.prefs[key]
	if !ok {
		return nil, settings.ErrNotFound
	}

	return &settings.Settings{
		Region: p.Region,
		Locale: p.Locale,
	}, nil
}

func (m *memory) SetRegion(_ context.Context, userID *commonpb.UserId, region *commonpb.Region) error {
	m.Lock()
	defer m.Unlock()

	key := string(userID.Value)

	p, ok := m.prefs[key]
	if !ok {
		p = &settings.Settings{
			Region: settings.DefaultRegion,
			Locale: settings.DefaultLocale,
		}
		m.prefs[key] = p
	}

	p.Region = region
	return nil
}

func (m *memory) SetLocale(_ context.Context, userID *commonpb.UserId, locale *commonpb.Locale) error {
	m.Lock()
	defer m.Unlock()

	key := string(userID.Value)

	p, ok := m.prefs[key]
	if !ok {
		p = &settings.Settings{
			Region: settings.DefaultRegion,
			Locale: settings.DefaultLocale,
		}
		m.prefs[key] = p
	}

	p.Locale = locale
	return nil
}

func (m *memory) reset() {
	m.Lock()
	defer m.Unlock()

	m.prefs = make(map[string]*settings.Settings)
}
