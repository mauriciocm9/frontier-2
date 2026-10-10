package storage

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
)

func (s *Store) deposit(ctx context.Context, tx pgx.Tx, op operation) (Result, error) {
	account := op.Args[0]
	balances, err := lockAccounts(ctx, tx, op, []string{account})
	if errors.Is(err, pgx.ErrNoRows) {
		return failure(404, "E401", "cuenta inexistente"), nil
	}
	if err != nil {
		return Result{}, err
	}
	if balances[account] > math.MaxInt64-op.Amount {
		return failure(400, "E303", "monto inválido"), nil
	}
	if err = journal(ctx, tx, op, map[string]int64{account: op.Amount, "__cash__": -op.Amount}); err != nil {
		return Result{}, err
	}
	return completePayment(ctx, tx, op)
}

func (s *Store) transfer(ctx context.Context, tx pgx.Tx, op operation) (Result, error) {
	source, destination := op.Args[0], op.Args[1]
	balances, err := lockAccounts(ctx, tx, op, []string{source, destination})
	if errors.Is(err, pgx.ErrNoRows) {
		return failure(404, "E401", "cuenta inexistente"), nil
	}
	if err != nil {
		return Result{}, err
	}
	if op.Amount > 100000 {
		return failure(422, "E506", "límite excedido"), nil
	}
	if balances[source] < op.Amount {
		return failure(422, "E507", "saldo insuficiente"), nil
	}
	if balances[destination] > math.MaxInt64-op.Amount {
		return failure(400, "E303", "monto inválido"), nil
	}
	if err = journal(ctx, tx, op, map[string]int64{source: -op.Amount, destination: op.Amount}); err != nil {
		return Result{}, err
	}
	return completePayment(ctx, tx, op)
}

func completePayment(ctx context.Context, tx pgx.Tx, op operation) (Result, error) {
	var n int64
	err := tx.QueryRow(ctx, "UPDATE tenants SET operation_counter=operation_counter+1 WHERE id=$1 RETURNING operation_counter", op.Tenant).Scan(&n)
	if err != nil {
		return Result{}, err
	}
	id := fmt.Sprintf("OP-%04d", n)
	_, err = tx.Exec(ctx, "UPDATE operations SET public_id=$2 WHERE request_id=$1", op.ID, id)
	return response(200, map[string]string{"status": "recieved", "operacionId": id}), err
}

func (s *Store) cancelOperation(ctx context.Context, tx pgx.Tx, op operation) (Result, error) {
	var id, status string
	err := tx.QueryRow(ctx, "SELECT request_id::text,status FROM operations WHERE tenant=$1 AND generation=$2 AND public_id=$3 FOR UPDATE", op.Tenant, op.Generation, op.Args[0]).Scan(&id, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return failure(404, "E402", "operación inexistente"), nil
	}
	if err != nil {
		return Result{}, err
	}
	if status == "cancelled" {
		return response(200, map[string]any{"ok": false, "motivo": "ya anulada"}), nil
	}
	rows, err := tx.Query(ctx, "SELECT account_id,delta FROM ledger_entries WHERE request_id=$1 ORDER BY account_id", id)
	if err != nil {
		return Result{}, err
	}
	deltas := make(map[string]int64)
	ids := []string{}
	for rows.Next() {
		var account string
		var delta int64
		if err = rows.Scan(&account, &delta); err != nil {
			rows.Close()
			return Result{}, err
		}
		deltas[account] = -delta
		if account != "__cash__" {
			ids = append(ids, account)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return Result{}, err
	}
	balances, err := lockAccounts(ctx, tx, op, ids)
	if err != nil {
		return Result{}, err
	}
	for account, balance := range balances {
		delta := deltas[account]
		if (delta > 0 && balance > math.MaxInt64-delta) || (delta < 0 && balance < math.MinInt64-delta) {
			return failure(400, "E303", "monto inválido"), nil
		}
	}
	// Compatibility: reversal is allowed to overdraw an account, as observed.
	if err = journal(ctx, tx, op, deltas); err != nil {
		return Result{}, err
	}
	_, err = tx.Exec(ctx, "UPDATE operations SET status='cancelled' WHERE request_id=$1", id)
	return response(200, map[string]any{"ok": true, "anulada": op.Args[0]}), err
}
