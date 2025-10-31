package postgres

import (
	"context"
	"strings"

	"github.com/knx-go/knx-go/knx/proxy"
)

type Store struct {
	conn *Conn
}

func OpenStore(conn string) (*Store, error) {
	trimmed := strings.TrimSpace(conn)
	if trimmed == "" {
		return nil, nil
	}

	pg, err := Dial(trimmed)
	if err != nil {
		return nil, err
	}

	if err := pg.EnsureSchema(); err != nil {
		pg.Close()
		return nil, err
	}

	return &Store{conn: pg}, nil
}

func (s *Store) Close() {
	if s == nil {
		return
	}
	if s.conn != nil {
		s.conn.Close()
	}
}

func (s *Store) Record(ctx context.Context, event proxy.Event) error {
	if s == nil {
		return nil
	}

	return s.conn.InsertEvent(ctx, event)
}

func (s *Store) Latest(ctx context.Context, destination string) (proxy.Event, bool, error) {
	if s == nil {
		return proxy.Event{}, false, nil
	}

	events, err := s.conn.SelectEvents(ctx, destination, 1)
	if err != nil {
		return proxy.Event{}, false, err
	}
	if len(events) == 0 {
		return proxy.Event{}, false, nil
	}
	return events[0], true, nil
}

func (s *Store) History(ctx context.Context, destination string, limit int) ([]proxy.Event, error) {
	if s == nil {
		return nil, nil
	}

	return s.conn.SelectEvents(ctx, destination, limit)
}
