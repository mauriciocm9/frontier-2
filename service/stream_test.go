package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"frontier/service/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

func TestConcurrentPublishersPreservePartitionOrder(t *testing.T) {
	s, tenant := testStore(t)
	accountTest(t, s, tenant)
	first, err := s.QueueDeposit(context.Background(), tenant, "", "deposit-1", "AC-0001", 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.QueueDeposit(context.Background(), tenant, "", "deposit-2", "AC-0001", 2)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var record struct{ Data []byte }
		if err := json.NewDecoder(r.Body).Decode(&record); err != nil {
			t.Error(err)
		}
		var e storage.Event
		if err := json.Unmarshal(record.Data, &e); err != nil {
			t.Error(err)
		}
		mu.Lock()
		calls = append(calls, e.RequestID)
		n := len(calls)
		mu.Unlock()
		if n == 1 {
			close(entered)
			<-release
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_, _ = w.Write([]byte(`{"SequenceNumber":"1","ShardId":"shard-1"}`))
	}))
	defer server.Close()
	client := kinesis.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	})}, func(options *kinesis.Options) { options.BaseEndpoint = aws.String(server.URL) })
	stream := &stream{client: client, name: "test", store: s}
	done := make(chan error, 1)
	go func() { _, err := stream.publishOne(context.Background()); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first publication did not start")
	}
	published, err := stream.publishOne(context.Background())
	close(release)
	if err != nil || published {
		t.Fatalf("second publisher overtook the first: %v %v", published, err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if published, err = stream.publishOne(context.Background()); err != nil || !published {
		t.Fatalf("next publication failed: %v %v", published, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 || calls[0] != first || calls[1] != second {
		t.Fatalf("incorrect partition order: %v", calls)
	}
}

func TestWorkerProcessesPaymentBatchInOrder(t *testing.T) {
	s, tenant := testStore(t)
	accountTest(t, s, tenant)
	accountTest(t, s, tenant)
	deposit, err := s.QueueDeposit(context.Background(), tenant, "", "deposit", "AC-0001", 100)
	if err != nil {
		t.Fatal(err)
	}
	transfer, err := s.QueueTransfer(context.Background(), tenant, "", "transfer", "AC-0001", "AC-0002", 100)
	if err != nil {
		t.Fatal(err)
	}
	cancel, err := s.QueueCancellation(context.Background(), tenant, "", "cancel", "OP-0002")
	if err != nil {
		t.Fatal(err)
	}
	shard := "batch-test"
	if err = s.EnsureShard(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	lease, claimed, err := s.ClaimShard(context.Background(), shard, "worker-1")
	if err != nil || !claimed {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	records := []map[string]string{}
	for i, id := range []string{deposit, transfer, cancel} {
		data, _ := json.Marshal(storage.Event{RequestID: id})
		records = append(records, map[string]string{"SequenceNumber": fmt.Sprint(i + 1), "Data": base64.StdEncoding.EncodeToString(data)})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_ = json.NewEncoder(w).Encode(map[string]any{"Records": records, "NextShardIterator": "next"})
	}))
	defer server.Close()
	client := kinesis.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	})}, func(options *kinesis.Options) { options.BaseEndpoint = aws.String(server.URL) })
	stream := &stream{client: client, name: "test", store: s}
	reader := &shardReader{lease: lease, iterator: aws.String("start")}
	if drained, err := stream.readBatch(context.Background(), reader); err != nil || drained {
		t.Fatalf("batch: %v %v", drained, err)
	}
	for _, id := range []string{deposit, transfer, cancel} {
		res, err := s.OperationResult(context.Background(), id, tenant)
		if err != nil || res == nil || res.Status != 200 {
			t.Fatalf("ordered operation failed: %v %v", res, err)
		}
	}
	for id, expected := range map[string]int64{"AC-0001": 100, "AC-0002": 0} {
		account, err := s.GetAccount(context.Background(), tenant, id)
		if err != nil || account.Balance != expected {
			t.Fatalf("%s balance: %v %v", id, account, err)
		}
	}
	if reader.lease.Checkpoint != "3" {
		t.Fatalf("checkpoint is %s, want 3", reader.lease.Checkpoint)
	}
	if err = s.ReleaseShard(context.Background(), reader.lease); err != nil {
		t.Fatal(err)
	}
	recovered, claimed, err := s.ClaimShard(context.Background(), shard, "worker-2")
	if err != nil || !claimed || recovered.Checkpoint != "3" {
		t.Fatalf("committed checkpoint not recovered: %v %v", recovered, err)
	}
}
