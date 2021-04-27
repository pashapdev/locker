package locker

import (
	"context"
	"errors"

	"database/sql"
)

type Locker interface {
	Lock(ctx context.Context, key1 int, key2 int) (Locker, error)
	UnLock(ctx context.Context, key1 int, key2 int) error
}

type pgLocker struct {
	db   *sql.DB
	conn *sql.Conn
}

func New(db *sql.DB) Locker {
	return &pgLocker{db: db}
}

func newWithConn(conn *sql.Conn) Locker {
	return &pgLocker{conn: conn}
}

func (l *pgLocker) Lock(ctx context.Context, key1 int, key2 int) (Locker, error) {
	var success bool

	conn, err := l.db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	q := "SELECT pg_try_advisory_lock($1, $2)"
	if err = conn.QueryRowContext(ctx, q, key1, key2).Scan(&success); err != nil {
		return nil, err
	}

	if !success {
		return nil, errors.New("failed to get lock")
	}

	return newWithConn(conn), nil
}

func (l *pgLocker) UnLock(ctx context.Context, key1 int, key2 int) error {
	var success bool

	defer func() {
		if l.conn != nil {
			l.conn.Close()
		}
	}()

	q := "SELECT pg_advisory_unlock($1, $2)"
	if err := l.conn.QueryRowContext(ctx, q, key1, key2).Scan(&success); err != nil {
		return err
	}

	if !success {
		return errors.New("failed to unlock")
	}

	return nil
}
