package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (s *Store) CreateAccount(ctx context.Context, tenant, name string) (Account, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Account{}, err
	}
	defer tx.Rollback(context.Background())
	generation, err := lockTenant(ctx, tx, tenant)
	if err != nil {
		return Account{}, err
	}
	var n int64
	if err = tx.QueryRow(ctx, "UPDATE tenants SET account_counter=account_counter+1 WHERE id=$1 RETURNING account_counter", tenant).Scan(&n); err != nil {
		return Account{}, err
	}
	a := Account{ID: fmt.Sprintf("AC-%04d", n), Name: name}
	_, err = tx.Exec(ctx, "INSERT INTO accounts(tenant,generation,id,name) VALUES($1,$2,$3,$4)", tenant, generation, a.ID, name)
	if err != nil {
		return Account{}, err
	}
	return a, tx.Commit(ctx)
}

func (s *Store) GetAccount(ctx context.Context, tenant, id string) (Account, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Account{}, err
	}
	defer tx.Rollback(context.Background())
	generation, err := lockTenant(ctx, tx, tenant)
	if err != nil {
		return Account{}, err
	}
	a := Account{ID: id}
	err = tx.QueryRow(ctx, "SELECT name,balance FROM accounts WHERE tenant=$1 AND generation=$2 AND id=$3", tenant, generation, id).Scan(&a.Name, &a.Balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrAccountNotFound
	}
	if err != nil {
		return Account{}, err
	}
	return a, tx.Commit(ctx)
}

func (s *Store) Reset(ctx context.Context, tenant string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = lockTenant(ctx, tx, tenant); err != nil {
		return err
	}
	for _, query := range []string{
		"DELETE FROM operations WHERE tenant=$1",
		"DELETE FROM accounts WHERE tenant=$1",
		"UPDATE tenants SET generation=generation+1, account_counter=0, operation_counter=0 WHERE id=$1",
	} {
		if _, err = tx.Exec(ctx, query, tenant); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
