package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"time"

	"frontier/service/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

type stream struct {
	client *kinesis.Client
	name   string
	store  *storage.Store
}

func (s *stream) publishOne(ctx context.Context) (bool, error) {
	return s.store.PublishNext(ctx, func(ctx context.Context, record storage.OutboxRecord) error {
		data, _ := json.Marshal(storage.Event{RequestID: record.RequestID})
		call, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err := s.client.PutRecord(call, &kinesis.PutRecordInput{StreamName: aws.String(s.name), PartitionKey: aws.String(record.PartitionKey), Data: data})
		return err
	})
}

func (s *stream) publisher(ctx context.Context) {
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		published, err := s.publishOne(attempt)
		cancel()
		if err != nil {
			slog.Error("outbox publication failed; retained for retry", "error", err)
		}
		if !published || err != nil {
			if !pause(ctx, 100*time.Millisecond) {
				return
			}
		}
	}
}

func pause(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *stream) shards(ctx context.Context) ([]types.Shard, error) {
	var shards []types.Shard
	input := &kinesis.ListShardsInput{StreamName: aws.String(s.name)}
	for {
		output, err := s.client.ListShards(ctx, input)
		if err != nil {
			return nil, err
		}
		shards = append(shards, output.Shards...)
		if output.NextToken == nil {
			break
		}
		input = &kinesis.ListShardsInput{NextToken: output.NextToken}
	}
	sort.Slice(shards, func(i, j int) bool { return aws.ToString(shards[i].ShardId) < aws.ToString(shards[j].ShardId) })
	return shards, nil
}

type shardReader struct {
	lease    storage.Lease
	iterator *string
}

// Exactly one loop processes records. No per-shard or per-record goroutines.
// Historical parent shards are read first during migration to the single shard.
func (s *stream) worker(ctx context.Context) error {
	owner := storage.NewID()
	readers := map[string]*shardReader{}
	defer func() {
		for _, reader := range readers {
			cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = s.store.ReleaseShard(cleanup, reader.lease)
			cancel()
		}
	}()
	var shards []types.Shard
	var nextDiscovery time.Time
	for ctx.Err() == nil {
		if time.Now().After(nextDiscovery) {
			attempt, cancel := context.WithTimeout(ctx, 4*time.Second)
			found, err := s.shards(attempt)
			cancel()
			if err != nil {
				slog.Error("shard discovery failed", "error", err)
				if !pause(ctx, time.Second) {
					break
				}
				continue
			}
			for _, shard := range found {
				if err = s.store.EnsureShard(ctx, aws.ToString(shard.ShardId)); err != nil {
					return err
				}
			}
			shards = found
			nextDiscovery = time.Now().Add(10 * time.Second)
		}
		for _, shard := range shards {
			id := aws.ToString(shard.ShardId)
			reader := readers[id]
			if reader == nil {
				var err error
				reader, err = s.claim(ctx, shard, owner)
				if err != nil {
					slog.Error("shard claim failed", "shard", id, "error", err)
					continue
				}
				if reader == nil {
					continue
				}
				readers[id] = reader
			}
			drained, err := s.readBatch(ctx, reader)
			if err != nil || drained {
				cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_ = s.store.ReleaseShard(cleanup, reader.lease)
				cancel()
				delete(readers, id)
				if err != nil && ctx.Err() == nil && !errors.Is(err, storage.ErrLeaseLost) {
					slog.Error("batch failed; resume from committed checkpoint", "shard", id, "error", err)
				}
			}
		}
		if !pause(ctx, 250*time.Millisecond) {
			break
		}
	}
	return nil
}

func (s *stream) claim(ctx context.Context, shard types.Shard, owner string) (*shardReader, error) {
	for _, parent := range []*string{shard.ParentShardId, shard.AdjacentParentShardId} {
		if parent != nil {
			drained, err := s.store.ParentDrained(ctx, *parent)
			if err != nil || !drained {
				return nil, err
			}
		}
	}
	l, claimed, err := s.store.ClaimShard(ctx, aws.ToString(shard.ShardId), owner)
	if err != nil || !claimed {
		return nil, err
	}
	iterator, err := s.iterator(ctx, l)
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.store.ReleaseShard(cleanup, l)
		return nil, err
	}
	slog.Info("sequential reader claimed shard", "shard", l.Shard, "epoch", l.Epoch)
	return &shardReader{lease: l, iterator: iterator}, nil
}

func (s *stream) iterator(ctx context.Context, l storage.Lease) (*string, error) {
	input := &kinesis.GetShardIteratorInput{StreamName: aws.String(s.name), ShardId: aws.String(l.Shard), ShardIteratorType: types.ShardIteratorTypeTrimHorizon}
	if l.Checkpoint != "" {
		input.ShardIteratorType = types.ShardIteratorTypeAfterSequenceNumber
		input.StartingSequenceNumber = aws.String(l.Checkpoint)
	}
	output, err := s.client.GetShardIterator(ctx, input)
	if err != nil {
		return nil, err
	}
	return output.ShardIterator, nil
}

func (s *stream) readBatch(ctx context.Context, reader *shardReader) (bool, error) {
	attempt, cancel := context.WithTimeout(ctx, 4*time.Second)
	if err := s.store.RenewShard(attempt, reader.lease); err != nil {
		cancel()
		return false, err
	}
	output, err := s.client.GetRecords(attempt, &kinesis.GetRecordsInput{ShardIterator: reader.iterator, Limit: aws.Int32(50)})
	cancel()
	if err != nil {
		return false, err
	}
	for _, record := range output.Records {
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		err = s.store.ProcessRecord(attempt, reader.lease, aws.ToString(record.SequenceNumber), record.Data)
		cancel()
		if err != nil {
			return false, err
		}
		reader.lease.Checkpoint = aws.ToString(record.SequenceNumber)
	}
	reader.iterator = output.NextShardIterator
	if reader.iterator == nil {
		return true, s.store.MarkShardDrained(ctx, reader.lease)
	}
	return false, nil
}
