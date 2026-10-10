# AWS deployment

One Go image runs as exactly one API task and one worker task, with one active Kinesis shard. The API handles HTTP, accounts and consultations; the worker processes DEPOSITO, TRANSFER and ANULA sequentially. Private RDS PostgreSQL stores business state, the outbox, ledger, shard leases and checkpoints.

## Current deployment

Region: `us-east-1`. Image: `sequential-20261010-01`. Endpoint:

```sh
export URL=http://frontier-api-1214499893.us-east-1.elb.amazonaws.com
export TOKEN='<team bearer token>'
curl --fail -H "Authorization: Bearer $TOKEN" "$URL/health"
# {"ok":true}
curl --fail -X POST -H "Authorization: Bearer $TOKEN" "$URL/__reset"
curl --fail -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{"m":"ALTA;Lucia;82"}' "$URL/msg"
# {"cuentaId":"AC-0001"}
curl --fail -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  --data '{"m":"DEPOSITO;AC-0001;100;82"}' "$URL/msg"
# {"status":"recieved","operacionId":"OP-0001"}
```

`GET /ready` is an unauthenticated database-readiness check for the ALB. The compatibility endpoints require bearer authentication. ALB uses HTTP initially; set `certificate_arn` and configure a matching domain for HTTPS.

## Terraform and releases

Requires Terraform >= 1.10, AWS CLI credentials, Docker with buildx, and Go 1.24 for local checks. Terraform state and deployment values are local and gitignored. Copy `deployment.tfvars.example` to `deployment.auto.tfvars` on another workstation, using the appropriate image tag. The current workstation uses `deployment.auto.tfvars.json`.

For a fresh environment, bootstrap RDS, the auth secret, IAM roles and ECR before creating ECS services. Set `deploy_services=false`; populate the token before switching it to true:

```sh
terraform -chdir=infra init
terraform -chdir=infra apply -var='deploy_services=false'
aws secretsmanager put-secret-value --region us-east-1 \
  --secret-id frontier-api-token --secret-string "$TOKEN"
REPOSITORY=$(terraform -chdir=infra output -raw repository_url)
REGION=$(terraform -chdir=infra output -raw region)
TAG=$(date -u +%Y%m%dT%H%M%SZ)
aws ecr get-login-password --region "$REGION" | \
  docker login --username AWS --password-stdin "${REPOSITORY%%/*}"
docker buildx build --platform linux/amd64 --provenance=false --push \
  -t "$REPOSITORY:$TAG" service
terraform -chdir=infra apply -var='deploy_services=true' -var="image_tag=$TAG"
```

Persist the chosen tag in the gitignored deployment values so later applies use the same image. Tags are immutable. For an existing deployment, push a new tag and apply the updated variables. ECS stops each old task before starting its replacement (minimum healthy 0%, maximum 100%), with a circuit breaker. This avoids overlapping workers but causes a brief interruption. Migrations run transactionally under a PostgreSQL advisory lock at startup.

RDS manages its password in Secrets Manager. ECS injects DB credentials and the bearer token using its execution role. Password values are not passed through Terraform state. The image includes the AWS RDS regional CA bundle and verifies database TLS certificates and hostname.

Move state to an encrypted S3 backend with `use_lockfile=true` before sharing deployment management; migrate the existing state rather than recreating resources. `.terraform.lock.hcl` belongs in version control.

## Network and database

- VPC `10.42.0.0/16`, public API/worker subnets in two availability zones, internet gateway, and public ALB.
- Fargate tasks have public IPs for outbound AWS access, avoiding NAT cost. Only the ALB security group can reach API port 8080; the worker has no inbound rules.
- RDS has dedicated private subnets (`10.42.10.0/24`, `10.42.11.0/24`), no internet route and no public endpoint. Port 5432 accepts only the API and worker security groups.
- PostgreSQL 16, `db.t4g.micro`, encrypted gp3 storage, 20 GB initial / 100 GB storage ceiling, seven-day automated backups, final snapshot on deletion. `database_multi_az` defaults to false and can enable failover.
- Both services currently use the managed database administrator credential for startup schema creation. A future migration/job role and restricted runtime roles can separate those privileges.

## Sequential processing

- Fixed counts: one API task, one worker task, one active shard. No task or shard autoscaling. Terraform manages desired counts, including correcting manual changes.
- The worker processes each batch and record synchronously, committing one before starting the next. Account creation and consultations remain in the API; reset/account operations still use PostgreSQL locks to coordinate with the consumer.
- A CloudWatch alarm monitors iterator age above 60s for two minutes; it does not scale workers. Throughput is bounded by this one processing loop.
- Shard lease epochs fence stale owners; checkpoint recovery survives task replacement. Historical parent shards drain before the child created when merging the former two shards. Closed parents may remain visible during Kinesis retention; the steady-state stream has one open shard.
- Kinesis is encrypted with 24-hour retention. Events contain only immutable operation UUIDs; the worker reads authoritative commands from PostgreSQL. Repeated stream records do not repeat effects.
- API outbox publishers serialize writes per tenant/source-account partition. HTTP callers wait up to 4.5s for the stored result. On timeout a pending 503 includes the request ID; the operation may still finish. Supplying `Idempotency-Key` allows safe retry; without it, identical client requests are separate operations.
- Reset atomically clears business state and increments a generation; old events cannot affect newly created accounts. Stream checkpoints and worker leases are retained across resets.
- Cancellation reverses deposits/transfers and can produce negative balances, matching the original as explicitly requested. Ordinary transfers require sufficient funds and are limited to 100000 per operation.
- Logs are retained in CloudWatch for 14 days. Malformed event envelopes go to PostgreSQL `dead_letters`; transient processing failures retry without advancing the checkpoint. No external payment provider is called.

## Verification

```sh
docker run --detach --name frontier-postgres-test \
  --publish 127.0.0.1:55432:5432 \
  --env POSTGRES_PASSWORD=test-password --env POSTGRES_DB=frontier postgres:16-alpine
```

From `service/`:

```sh
TEST_DATABASE_URL='postgres://postgres:test-password@127.0.0.1:55432/frontier?sslmode=disable' \
  go test -race -count=1 -v ./...
go vet ./...
```

From the repository root:

```sh
python3 pruebas/verify_deployment.py
python3 pruebas/verify_deployment.py --redeliver
# Also contacts the original and logs each request, respecting its rate limit:
python3 pruebas/verify_deployment.py --differential
```

Tests reset the deployed business state. The previous distributed deployment passed the fixture, current-original, retry, concurrency, cancellation, real-redelivery and reset checks. The refactor also tests the sequential deposit/transfer/cancellation batch and persisted checkpoint. See `service/storage/README.md` for the explicit storage API.

## Diagram

See the updated Mermaid architecture in `docs/design.md`. The earlier PDF exports predate this implementation.

To tear down, remove ECR images first, then `terraform -chdir=infra destroy` with the same deployment variables. RDS creates a final snapshot; the bearer secret retains its recovery window.
