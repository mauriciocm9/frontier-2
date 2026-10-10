package storage

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// These explicit entrypoints persist intent and an outbox event atomically.
// The sequential worker applies the effect through the corresponding handler.
func (s *Store) QueueDeposit(ctx context.Context, tenant, key, raw, account string, amount int64) (string, error) {
	if amount <= 0 {
		return "", errors.New("deposit amount must be positive")
	}
	return s.enqueue(ctx, tenant, key, operationRequest{Kind: "DEPOSITO", Args: []string{account}, Amount: amount, Raw: raw})
}

func (s *Store) QueueTransfer(ctx context.Context, tenant, key, raw, source, destination string, amount int64) (string, error) {
	if amount <= 0 || source == destination {
		return "", errors.New("transfer requires positive amount and different accounts")
	}
	return s.enqueue(ctx, tenant, key, operationRequest{Kind: "TRANSFER", Args: []string{source, destination}, Amount: amount, Raw: raw})
}

func (s *Store) QueueCancellation(ctx context.Context, tenant, key, raw, operationID string) (string, error) {
	return s.enqueue(ctx, tenant, key, operationRequest{Kind: "ANULA", Args: []string{operationID}, Raw: raw})
}

type operationRequest struct {
	Kind, Raw string
	Args      []string
	Amount    int64
}

type operation struct {
	ID, Tenant, Kind, Status string
	Generation, Amount       int64
	Args                     []string
}

func (s *Store) enqueue(ctx context.Context, tenant, key string, c operationRequest) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(context.Background())
	generation, err := lockTenant(ctx, tx, tenant)
	if err != nil {
		return "", err
	}
	id := NewID()
	if key == "" {
		key = "auto:" + id
	} else {
		key = "client:" + key
	}
	var existing, fingerprint string
	err = tx.QueryRow(ctx, "SELECT request_id::text,fingerprint FROM operations WHERE tenant=$1 AND generation=$2 AND request_key=$3", tenant, generation, key).Scan(&existing, &fingerprint)
	if err == nil {
		if fingerprint != Hash(c.Raw) {
			return "", ErrIdempotencyConflict
		}
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	_, err = tx.Exec(ctx, "INSERT INTO operations(request_id,tenant,generation,request_key,fingerprint,command,args,amount) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", id, tenant, generation, key, Hash(c.Raw), c.Kind, c.Args, c.Amount)
	if err != nil {
		return "", err
	}
	partition := tenant + ":" + c.Args[0]
	if c.Kind == "ANULA" {
		var args []string
		err = tx.QueryRow(ctx, "SELECT args FROM operations WHERE tenant=$1 AND generation=$2 AND public_id=$3", tenant, generation, c.Args[0]).Scan(&args)
		if err == nil {
			partition = tenant + ":" + args[0]
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return "", err
		}
	}
	_, err = tx.Exec(ctx, "INSERT INTO outbox(request_id,partition_key) VALUES($1,$2)", id, partition)
	if err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

func (s *Store) OperationResult(ctx context.Context, id, tenant string) (*Result, error) {
	var status *int
	var body []byte
	err := s.pool.QueryRow(ctx, "SELECT http_status,response FROM operations WHERE request_id=$1 AND tenant=$2", id, tenant).Scan(&status, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrResetInvalidated
	}
	if err != nil || status == nil {
		return nil, err
	}
	return &Result{Status: *status, Body: body}, nil
}
