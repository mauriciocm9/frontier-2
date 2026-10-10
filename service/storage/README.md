# PostgreSQL storage

This package owns SQL and database transactions. The API and Kinesis client do not access the connection pool directly.

## Explicit operations

| Method | Purpose |
| --- | --- |
| `CreateAccount(ctx, tenant, name)` | Allocate an AC ID and insert an account atomically. |
| `GetAccount(ctx, tenant, id)` | Return the typed account and its committed balance. |
| `QueueDeposit(ctx, tenant, key, raw, account, amount)` | Persist deposit intent plus its outbox record. |
| `QueueTransfer(ctx, tenant, key, raw, source, destination, amount)` | Persist transfer intent plus its outbox record. |
| `QueueCancellation(ctx, tenant, key, raw, operationID)` | Persist cancellation intent plus its outbox record. |
| `OperationResult(ctx, requestID, tenant)` | Read the saved result, or nil while it is pending. |
| `Reset(ctx, tenant)` | Clear business data, restart public counters and fence old events. |

`ProcessRecord` handles the stream envelope, ownership and checkpoint transaction, then explicitly dispatches to `deposit`, `transfer`, or `cancelOperation` in `payments.go`. Each handler spells out its validation and balance changes; only ledger and identifier helpers are shared.

`PublishNext` protects an outbox record while the caller publishes it. `leases.go` implements claim, renewal, release and parent-drain checks. The pool stays private. Stored compatibility responses are represented by `Result`; ordinary account methods return `Account` and typed errors instead of HTTP responses.

The single worker calls `ProcessRecord` synchronously for each Kinesis record. PostgreSQL transactions, idempotency and fencing remain necessary for retries, restart, reset and overlapping HTTP requests.
