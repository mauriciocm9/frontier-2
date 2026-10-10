"""Replay the fixture and verify distributed money movement on the deployment."""
import argparse
import base64
import concurrent.futures
import json
import hashlib
import subprocess
import time
import urllib.error
import urllib.request

from probe_behavior import ROOT, TOKEN, control, request as original


parser = argparse.ArgumentParser()
parser.add_argument("--url", default="http://frontier-api-1214499893.us-east-1.elb.amazonaws.com")
parser.add_argument("--differential", action="store_true")
parser.add_argument("--redeliver", action="store_true", help="Use AWS CLI to redeliver a real stream event and check reset fencing")
args = parser.parse_args()


def call(path, body=None, method="POST", key=None):
    headers = {"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"}
    if key:
        headers["Idempotency-Key"] = key
    req = urllib.request.Request(args.url + path, data=json.dumps(body).encode() if body is not None else None, method=method, headers=headers)
    start = time.monotonic()
    try:
        resp = urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as error:
        resp = error
    return resp.status, json.loads(resp.read()), resp.headers.get("X-Request-ID"), time.monotonic() - start


def msg(prefix, key=None):
    return call("/msg", {"m": f"{prefix};{control(prefix)}"}, key=key)


assert call("/health", method="GET")[:2] == (200, {"ok": True})
assert call("/__reset")[:2] == (200, {"ok": True})
if args.differential:
    original("/__reset", hypothesis="Differential replay baseline", seq="deployed-replay")
passed = 0
for line in (ROOT / "pruebas/ejemplos.jsonl").read_text().splitlines():
    example = json.loads(line)
    actual = call("/msg", example["request"])
    expected = (example["status"], example["response"])
    assert actual[:2] == expected, (example["n"], actual, expected)
    if args.differential:
        live = original("/msg", example["request"], hypothesis="Deployed implementation matches current original", seq="deployed-replay")
        assert actual[:2] == live, (example["n"], actual, live)
    passed += 1
    print(f"fixture {example['n']:02d} matched ({actual[3]:.3f}s)", flush=True)
print(f"fixture: {passed}/20 matched", flush=True)

assert call("/__reset")[0] == 200
assert msg("ALTA;A")[:2] == (201, {"cuentaId": "AC-0001"})
assert msg("ALTA;B")[:2] == (201, {"cuentaId": "AC-0002"})
deposit = msg("DEPOSITO;AC-0001;100", key="smoke-deposit")
assert deposit[:2] == (200, {"status": "recieved", "operacionId": "OP-0001"}), deposit
assert msg("DEPOSITO;AC-0001;100", key="smoke-deposit")[:3] == deposit[:3]
assert msg("DEPOSITO;AC-0001;101", key="smoke-deposit")[0] == 409
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    results = list(executor.map(lambda _: msg("TRANSFER;AC-0001;AC-0002;100"), range(2)))
assert sorted(res[0] for res in results) == [200, 422], results
assert msg("CONSULTA;AC-0001")[1]["saldo"] == 0
assert msg("CONSULTA;AC-0002")[1]["saldo"] == 100
assert msg("ANULA;OP-0002")[:2] == (200, {"ok": True, "anulada": "OP-0002"})
assert msg("ANULA;OP-0002")[:2] == (200, {"ok": False, "motivo": "ya anulada"})
assert msg("CONSULTA;AC-0001")[1]["saldo"] == 100
assert msg("CONSULTA;AC-0002")[1]["saldo"] == 0
print("idempotent retries, key conflict, concurrent spending and cancellation: passed", flush=True)

# Two identical requests without a key must produce two effects.
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(lambda _: msg("DEPOSITO;AC-0001;1"), range(4)))
assert all(res[0] == 200 for res in results), results
assert len({res[1]["operacionId"] for res in results}) == 4
assert msg("CONSULTA;AC-0001")[1]["saldo"] == 104
print("four concurrent deposits without a key: four distinct operations", flush=True)
assert call("/__reset")[0] == 200
assert msg("ALTA;After reset")[:2] == (201, {"cuentaId": "AC-0001"})
assert msg("CONSULTA;AC-0001")[1]["saldo"] == 0
print("reset and counter restart: passed", flush=True)

if args.redeliver:
    deposit = msg("DEPOSITO;AC-0001;10", key="stream-redelivery")
    assert deposit[0] == 200, deposit
    payload = base64.b64encode(json.dumps({"request_id": deposit[2]}).encode()).decode()
    partition = hashlib.sha256(TOKEN.encode()).hexdigest() + ":AC-0001"

    def redeliver():
        subprocess.run(["aws", "kinesis", "put-record", "--region", "us-east-1", "--stream-name", "frontier-operations", "--partition-key", partition, "--data", payload, "--query", "SequenceNumber", "--output", "text"], check=True, capture_output=True)
        time.sleep(1)

    redeliver()
    assert msg("CONSULTA;AC-0001")[1]["saldo"] == 10
    assert call("/__reset")[0] == 200
    assert msg("ALTA;New generation")[0] == 201
    redeliver()
    assert msg("CONSULTA;AC-0001")[1]["saldo"] == 0
    print("real Kinesis redelivery and stale event after reset: passed", flush=True)

assert call("/__reset")[0] == 200
print("deployment left with empty business state", flush=True)
