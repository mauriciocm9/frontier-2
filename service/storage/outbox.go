package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

type OutboxRecord struct {
	RequestID, PartitionKey string
}

// PublishNext protects publication order; only outbox locks span the network call.
// A crash after sending but before commit can repeat the UUID, never its effect.
func (s *Store) PublishNext(ctx context.Context, send func(context.Context, OutboxRecord) error) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	var record OutboxRecord
	err = tx.QueryRow(ctx, "SELECT partition_key FROM outbox WHERE published_at IS NULL ORDER BY created_at,request_id LIMIT 1").Scan(&record.PartitionKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var acquired bool
	if err = tx.QueryRow(ctx, "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))", record.PartitionKey).Scan(&acquired); err != nil || !acquired {
		return false, err
	}
	err = tx.QueryRow(ctx, "SELECT request_id::text FROM outbox WHERE published_at IS NULL AND partition_key=$1 ORDER BY created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1", record.PartitionKey).Scan(&record.RequestID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err = send(ctx, record); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, "UPDATE outbox SET published_at=clock_timestamp() WHERE request_id=$1", record.RequestID); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}
