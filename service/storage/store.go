// Package storage owns PostgreSQL access, transaction boundaries and the ledger.
// HTTP parsing and Kinesis network calls live outside this package.
package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string

type Store struct{ pool *pgxpool.Pool }

type Account struct {
	ID      string
	Name    string
	Balance int64
}

// Result is the saved compatibility response for an asynchronous operation.
type Result struct {
	Status int
	Body   json.RawMessage
}

type Event struct {
	RequestID string `json:"request_id"`
}

type Lease struct {
	Shard, Owner, Checkpoint string
	Epoch                    int64
}

var (
	ErrAccountNotFound     = errors.New("account not found")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different input")
	ErrResetInvalidated    = errors.New("operation invalidated by reset")
	ErrLeaseLost           = errors.New("shard lease lost")
)

func Open(ctx context.Context, dsn string) (*Store, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 8
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	config.ConnConfig.RuntimeParams["statement_timeout"] = "3000"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	s := &Store{pool: pool}
	if err = s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(420001)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6], b[8] = (b[6]&0x0f)|0x40, (b[8]&0x3f)|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func Hash(text string) string {
	b := sha256.Sum256([]byte(text))
	return hex.EncodeToString(b[:])
}

func response(status int, body any) Result {
	b, _ := json.Marshal(body)
	return Result{Status: status, Body: b}
}

func failure(status int, code, message string) Result {
	return response(status, map[string]string{"error": code, "mensaje": message})
}

// Tenant locks serialize public ID allocation and fence reset. Ordered account
// locks additionally protect balance changes and future callers of this package.
func lockTenant(ctx context.Context, tx pgx.Tx, tenant string) (int64, error) {
	_, err := tx.Exec(ctx, "INSERT INTO tenants(id) VALUES($1) ON CONFLICT DO NOTHING", tenant)
	if err != nil {
		return 0, err
	}
	var generation int64
	err = tx.QueryRow(ctx, "SELECT generation FROM tenants WHERE id=$1 FOR UPDATE", tenant).Scan(&generation)
	return generation, err
}
