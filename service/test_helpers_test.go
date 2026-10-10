package main

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"

	"frontier/service/storage"

	"github.com/jackc/pgx/v5"
)

func testStore(t *testing.T) (*storage.Store, string) {
	t.Helper()
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	// Each application test has its own schema, so its outbox publisher cannot
	// pick up operations from storage tests running in another Go package.
	admin, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + strings.ReplaceAll(storage.NewID(), "-", "")
	if _, err = admin.Exec(context.Background(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		admin.Close(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		_ = admin.Close(context.Background())
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	s, err := storage.Open(context.Background(), u.String())
	if err != nil {
		t.Fatal(err)
	}
	tenant := "api-test:" + storage.NewID()
	t.Cleanup(func() {
		_ = s.Reset(context.Background(), tenant)
		s.Close()
	})
	return s, tenant
}

func accountTest(t *testing.T, s *storage.Store, tenant string) {
	t.Helper()
	if _, err := s.CreateAccount(context.Background(), tenant, "Test"); err != nil {
		t.Fatal(err)
	}
}
