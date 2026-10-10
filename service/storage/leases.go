package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

func (s *Store) EnsureShard(ctx context.Context, shard string) error {
	_, err := s.pool.Exec(ctx, "INSERT INTO shard_leases(shard_id,owner,epoch,expires_at) VALUES($1,'',0,'1970-01-01') ON CONFLICT DO NOTHING", shard)
	return err
}

func (s *Store) ParentDrained(ctx context.Context, parent string) (bool, error) {
	var unfinished bool
	err := s.pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM shard_leases WHERE shard_id=$1 AND NOT drained)", parent).Scan(&unfinished)
	return !unfinished, err
}

func (s *Store) ClaimShard(ctx context.Context, shard, owner string) (Lease, bool, error) {
	l := Lease{Shard: shard, Owner: owner}
	err := s.pool.QueryRow(ctx, `INSERT INTO shard_leases(shard_id,owner,expires_at)
 VALUES($1,$2,clock_timestamp()+interval '15 seconds')
 ON CONFLICT(shard_id) DO UPDATE SET owner=EXCLUDED.owner,
 epoch=shard_leases.epoch+1,expires_at=EXCLUDED.expires_at
 WHERE shard_leases.expires_at<=clock_timestamp() AND NOT shard_leases.drained
 RETURNING epoch,checkpoint`, shard, owner).Scan(&l.Epoch, &l.Checkpoint)
	if errors.Is(err, pgx.ErrNoRows) {
		return Lease{}, false, nil
	}
	return l, err == nil, err
}

func (s *Store) RenewShard(ctx context.Context, l Lease) error {
	updated, err := s.pool.Exec(ctx, "UPDATE shard_leases SET expires_at=clock_timestamp()+interval '15 seconds' WHERE shard_id=$1 AND owner=$2 AND epoch=$3 AND expires_at>clock_timestamp()", l.Shard, l.Owner, l.Epoch)
	if err == nil && updated.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return err
}

func (s *Store) ReleaseShard(ctx context.Context, l Lease) error {
	_, err := s.pool.Exec(ctx, "UPDATE shard_leases SET expires_at=clock_timestamp() WHERE shard_id=$1 AND owner=$2 AND epoch=$3", l.Shard, l.Owner, l.Epoch)
	return err
}

func (s *Store) MarkShardDrained(ctx context.Context, l Lease) error {
	updated, err := s.pool.Exec(ctx, "UPDATE shard_leases SET drained=true WHERE shard_id=$1 AND owner=$2 AND epoch=$3 AND expires_at>clock_timestamp()", l.Shard, l.Owner, l.Epoch)
	if err == nil && updated.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return err
}
