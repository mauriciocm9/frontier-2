package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
)

func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL transaction tests")
	}
	s, err := Open(context.Background(), os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	tenant := "test:" + NewID()
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), "DELETE FROM operations WHERE tenant=$1", tenant)
		_, _ = s.pool.Exec(context.Background(), "DELETE FROM accounts WHERE tenant=$1", tenant)
		_, _ = s.pool.Exec(context.Background(), "DELETE FROM tenants WHERE id=$1", tenant)
		s.pool.Close()
	})
	return s, tenant
}

func testCommand(t *testing.T, prefix string) operationRequest {
	t.Helper()
	parts := strings.Split(prefix, ";")
	c := operationRequest{Kind: parts[0], Args: parts[1:], Raw: prefix}
	if c.Kind == "DEPOSITO" || c.Kind == "TRANSFER" {
		amount, err := strconv.ParseInt(c.Args[len(c.Args)-1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		c.Amount = amount
		c.Args = c.Args[:len(c.Args)-1]
	}
	return c
}

func testLease(t *testing.T, s *Store) Lease {
	t.Helper()
	l := Lease{Shard: "test:" + NewID(), Owner: NewID(), Epoch: 1}
	_, err := s.pool.Exec(context.Background(), "INSERT INTO shard_leases(shard_id,owner,epoch,expires_at) VALUES($1,$2,$3,clock_timestamp()+interval '1 hour')", l.Shard, l.Owner, l.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), "DELETE FROM shard_leases WHERE shard_id=$1", l.Shard)
	})
	return l
}

func enqueueTest(t *testing.T, s *Store, tenant, key, prefix string) string {
	t.Helper()
	c := testCommand(t, prefix)
	var id string
	var err error
	switch c.Kind {
	case "DEPOSITO":
		id, err = s.QueueDeposit(context.Background(), tenant, key, c.Raw, c.Args[0], c.Amount)
	case "TRANSFER":
		id, err = s.QueueTransfer(context.Background(), tenant, key, c.Raw, c.Args[0], c.Args[1], c.Amount)
	case "ANULA":
		id, err = s.QueueCancellation(context.Background(), tenant, key, c.Raw, c.Args[0])
	}
	if err != nil {
		t.Fatalf("enqueue %s: %v", prefix, err)
	}
	return id
}

func consumeTest(s *Store, l Lease, id string) error {
	b, _ := json.Marshal(Event{RequestID: id})
	return s.ProcessRecord(context.Background(), l, "123", b)
}

func completeTest(t *testing.T, s *Store, tenant, prefix string) Result {
	t.Helper()
	id := enqueueTest(t, s, tenant, "", prefix)
	l := testLease(t, s)
	if err := consumeTest(s, l, id); err != nil {
		t.Fatal(err)
	}
	res, err := s.OperationResult(context.Background(), id, tenant)
	if err != nil || res == nil {
		t.Fatalf("outcome: %v %v", res, err)
	}
	return *res
}

func accountTest(t *testing.T, s *Store, tenant string) {
	t.Helper()
	res, err := s.CreateAccount(context.Background(), tenant, "Test")
	if err != nil || res.ID == "" {
		t.Fatalf("account: %v %v", res, err)
	}
}

func balanceTest(t *testing.T, s *Store, tenant, account string, expected int64) {
	t.Helper()
	res, err := s.GetAccount(context.Background(), tenant, account)
	if err != nil {
		t.Fatal(err)
	}
	if res.Balance != expected {
		t.Fatalf("%s balance: %d, want %d", account, res.Balance, expected)
	}
}

func TestDuplicateDeliveryAndIdempotency(t *testing.T) {
	s, tenant := testStore(t)
	accountTest(t, s, tenant)
	id := enqueueTest(t, s, tenant, "deposit-1", "DEPOSITO;AC-0001;100")
	if again := enqueueTest(t, s, tenant, "deposit-1", "DEPOSITO;AC-0001;100"); again != id {
		t.Fatal("retry allocated a new operation")
	}
	l := testLease(t, s)
	var wg sync.WaitGroup
	failures := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); failures <- consumeTest(s, l, id) }()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	balanceTest(t, s, tenant, "AC-0001", 100)
	_, err := s.QueueDeposit(context.Background(), tenant, "deposit-1", "DEPOSITO;AC-0001;101", "AC-0001", 101)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatal("reused key with different body did not conflict")
	}
	// Same contents without a client key is intentionally a new operation.
	if res := completeTest(t, s, tenant, "DEPOSITO;AC-0001;100"); res.Status != 200 {
		t.Fatal(string(res.Body))
	}
	balanceTest(t, s, tenant, "AC-0001", 200)
}

func TestConcurrentSpendingAndCancellation(t *testing.T) {
	s, tenant := testStore(t)
	accountTest(t, s, tenant)
	accountTest(t, s, tenant)
	completeTest(t, s, tenant, "DEPOSITO;AC-0001;100")
	ids := []string{enqueueTest(t, s, tenant, "", "TRANSFER;AC-0001;AC-0002;100"), enqueueTest(t, s, tenant, "", "TRANSFER;AC-0001;AC-0002;100")}
	leases := []Lease{testLease(t, s), testLease(t, s)}
	var wg sync.WaitGroup
	errors := make(chan error, 2)
	for i := range ids {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errors <- consumeTest(s, leases[i], ids[i]) }(i)
	}
	wg.Wait()
	for range ids {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	successes := 0
	for _, id := range ids {
		res, err := s.OperationResult(context.Background(), id, tenant)
		if err != nil || res == nil {
			t.Fatal(err)
		}
		if res.Status == 200 {
			successes++
		} else if res.Status != 422 {
			t.Fatal(string(res.Body))
		}
	}
	if successes != 1 {
		t.Fatalf("%d concurrent spends succeeded; want 1", successes)
	}
	balanceTest(t, s, tenant, "AC-0001", 0)
	balanceTest(t, s, tenant, "AC-0002", 100)
	first := completeTest(t, s, tenant, "ANULA;OP-0002")
	second := completeTest(t, s, tenant, "ANULA;OP-0002")
	if string(first.Body) == string(second.Body) {
		t.Fatal("repeated cancellation did not return already-cancelled")
	}
	balanceTest(t, s, tenant, "AC-0001", 100)
	balanceTest(t, s, tenant, "AC-0002", 0)
	var unbalanced int
	if err := s.pool.QueryRow(context.Background(), "SELECT count(*) FROM (SELECT e.request_id FROM ledger_entries e JOIN operations o USING(request_id) WHERE o.tenant=$1 GROUP BY e.request_id HAVING sum(delta)<>0) x", tenant).Scan(&unbalanced); err != nil || unbalanced != 0 {
		t.Fatalf("unbalanced ledger: %d %v", unbalanced, err)
	}
}

func TestCancellationMatchesOriginalOverdraft(t *testing.T) {
	s, tenant := testStore(t)
	accountTest(t, s, tenant)
	accountTest(t, s, tenant)
	completeTest(t, s, tenant, "DEPOSITO;AC-0001;100")
	completeTest(t, s, tenant, "TRANSFER;AC-0001;AC-0002;100")
	completeTest(t, s, tenant, "ANULA;OP-0001")
	balanceTest(t, s, tenant, "AC-0001", -100)
	completeTest(t, s, tenant, "ANULA;OP-0002")
	balanceTest(t, s, tenant, "AC-0001", 0)
	balanceTest(t, s, tenant, "AC-0002", 0)
}

func TestResetFencesQueuedEventsAndExpiredLeases(t *testing.T) {
	s, tenant := testStore(t)
	accountTest(t, s, tenant)
	old := enqueueTest(t, s, tenant, "", "DEPOSITO;AC-0001;100")
	l := testLease(t, s)
	if err := s.Reset(context.Background(), tenant); err != nil {
		t.Fatal(err)
	}
	accountTest(t, s, tenant)
	if err := consumeTest(s, l, old); err != nil {
		t.Fatal(err)
	}
	balanceTest(t, s, tenant, "AC-0001", 0)
	id := enqueueTest(t, s, tenant, "", "DEPOSITO;AC-0001;100")
	_, _ = s.pool.Exec(context.Background(), "UPDATE shard_leases SET epoch=epoch+1 WHERE shard_id=$1", l.Shard)
	if err := consumeTest(s, l, id); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale owner was not fenced: %v", err)
	}
	balanceTest(t, s, tenant, "AC-0001", 0)
	_, _ = s.pool.Exec(context.Background(), "UPDATE shard_leases SET epoch=$2, expires_at=clock_timestamp()-interval '1 second' WHERE shard_id=$1", l.Shard, l.Epoch)
	if err := consumeTest(s, l, id); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired owner was not fenced: %v", err)
	}
}

func TestUnbalancedJournalRollsBack(t *testing.T) {
	s, tenant := testStore(t)
	accountTest(t, s, tenant)
	id := enqueueTest(t, s, tenant, "", "DEPOSITO;AC-0001;100")
	tx, err := s.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	var generation int64
	if generation, err = lockTenant(context.Background(), tx, tenant); err != nil {
		t.Fatal(err)
	}
	op := operation{ID: id, Tenant: tenant, Generation: generation}
	if err = journal(context.Background(), tx, op, map[string]int64{"AC-0001": 100}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(context.Background()); err == nil || errors.Is(err, pgx.ErrTxClosed) {
		t.Fatal("unbalanced entry was committed")
	}
	balanceTest(t, s, tenant, "AC-0001", 0)
	var entries int
	if err = s.pool.QueryRow(context.Background(), "SELECT count(*) FROM ledger_entries WHERE request_id=$1", id).Scan(&entries); err != nil || entries != 0 {
		t.Fatalf("partial journal remained: %d %v", entries, err)
	}
}
