package storage

import (
	"context"
	"sort"

	"github.com/jackc/pgx/v5"
)

func lockAccounts(ctx context.Context, tx pgx.Tx, op operation, ids []string) (map[string]int64, error) {
	ordered := append([]string(nil), ids...)
	sort.Strings(ordered)
	balances := make(map[string]int64)
	for _, id := range ordered {
		var balance int64
		err := tx.QueryRow(ctx, "SELECT balance FROM accounts WHERE tenant=$1 AND generation=$2 AND id=$3 FOR UPDATE", op.Tenant, op.Generation, id).Scan(&balance)
		if err != nil {
			return nil, err
		}
		balances[id] = balance
	}
	return balances, nil
}

func journal(ctx context.Context, tx pgx.Tx, op operation, deltas map[string]int64) error {
	keys := make([]string, 0, len(deltas))
	for key := range deltas {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, id := range keys {
		delta := deltas[id]
		if id != "__cash__" {
			if _, err := tx.Exec(ctx, "UPDATE accounts SET balance=balance+$4 WHERE tenant=$1 AND generation=$2 AND id=$3", op.Tenant, op.Generation, id, delta); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, "INSERT INTO ledger_entries(request_id,account_id,delta) VALUES($1,$2,$3)", op.ID, id, delta); err != nil {
			return err
		}
	}
	return nil
}
