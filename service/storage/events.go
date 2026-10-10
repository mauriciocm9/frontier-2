package storage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ProcessRecord commits one effect, its response and its checkpoint together.
// Replayed UUIDs reuse their stored result, and events predating reset are no-ops.
func (s *Store) ProcessRecord(ctx context.Context, l Lease, sequence string, data []byte) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var owned bool
	err = tx.QueryRow(ctx, "SELECT owner=$2 AND epoch=$3 AND expires_at>clock_timestamp() FROM shard_leases WHERE shard_id=$1 FOR UPDATE", l.Shard, l.Owner, l.Epoch).Scan(&owned)
	if err != nil {
		return err
	}
	if !owned {
		return ErrLeaseLost
	}
	var e Event
	if err = json.Unmarshal(data, &e); err != nil || !validUUID(e.RequestID) {
		_, err = tx.Exec(ctx, "INSERT INTO dead_letters(shard_id,sequence_number,payload,reason) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", l.Shard, sequence, data, "invalid event envelope")
	} else {
		err = s.processOperation(ctx, tx, e.RequestID)
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE shard_leases SET checkpoint=$2 WHERE shard_id=$1", l.Shard, sequence); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) processOperation(ctx context.Context, tx pgx.Tx, requestID string) error {
	var op operation
	err := tx.QueryRow(ctx, "SELECT request_id::text,tenant,generation,command,args,amount,status FROM operations WHERE request_id=$1", requestID).Scan(&op.ID, &op.Tenant, &op.Generation, &op.Kind, &op.Args, &op.Amount, &op.Status)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && op.Status != "received") {
		return nil
	}
	if err != nil {
		return err
	}
	generation, err := lockTenant(ctx, tx, op.Tenant)
	if err != nil {
		return err
	}
	// Recheck after the tenant lock; reset or another delivery may have won.
	err = tx.QueryRow(ctx, "SELECT status FROM operations WHERE request_id=$1 FOR UPDATE", op.ID).Scan(&op.Status)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && (generation != op.Generation || op.Status != "received")) {
		return nil
	}
	if err != nil {
		return err
	}
	var res Result
	switch op.Kind {
	case "DEPOSITO":
		res, err = s.deposit(ctx, tx, op)
	case "TRANSFER":
		res, err = s.transfer(ctx, tx, op)
	case "ANULA":
		res, err = s.cancelOperation(ctx, tx, op)
	default:
		res = failure(400, "E301", "comando desconocido ")
	}
	if err != nil {
		return err
	}
	state := "completed"
	if res.Status >= 400 {
		state = "failed"
	}
	_, err = tx.Exec(ctx, "UPDATE operations SET status=$2,http_status=$3,response=$4,completed_at=clock_timestamp() WHERE request_id=$1", op.ID, state, res.Status, res.Body)
	return err
}

func validUUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil
}
