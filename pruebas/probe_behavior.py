"""Targeted, rate-limited black-box probes; every request is logged."""
import json
import pathlib
import re
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
instructions = (ROOT / "AGENTS.md").read_text()
URL = re.search(r"API_URL = (\S+)", instructions).group(1)
TOKEN = re.search(r"TOKEN = (\S+)", instructions).group(1)
LOG = ROOT / "experimentos.jsonl"
counter = sum(1 for _ in LOG.open())


def control(prefix):
    remainder = 0
    for char in prefix.upper():
        digits = str(ord(char) - 64) if "A" <= char <= "Z" else char if "0" <= char <= "9" else ""
        for digit in digits:
            remainder = (10 * remainder + int(digit)) % 97
    return 98 - remainder * 100 % 97


def request(path, body=None, method="POST", token=TOKEN, hypothesis="", seq="behavior-v1", raw=None):
    global counter
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(URL + path, data=raw if raw is not None else json.dumps(body).encode() if body is not None else None, method=method, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=5)
    except urllib.error.HTTPError as error:
        resp = error
    response_raw = resp.read().decode()
    try:
        result = json.loads(response_raw)
    except json.JSONDecodeError:
        result = response_raw
    counter += 1
    with LOG.open("a") as out:
        out.write(json.dumps({"id": counter, "seq": seq, "x_req": resp.headers.get("X-Req"), "hipotesis": hypothesis, "path": path, "method": method, "request": raw.decode() if raw is not None else body, "status": resp.status, "response": result}, ensure_ascii=False) + "\n")
    print(path, body, resp.status, json.dumps(result, ensure_ascii=False), flush=True)
    time.sleep(0.15)
    return resp.status, result


def msg(prefix, hypothesis):
    return request("/msg", {"m": f"{prefix};{control(prefix)}"}, hypothesis=hypothesis)


if __name__ == "__main__":
    request("/health", method="GET", hypothesis="Exact health body")
    request("/health", method="GET", token=None, hypothesis="Health authentication")
    request("/msg", {"m": "PING;64"}, token="wrong", hypothesis="Invalid token response")
    request("/missing", method="GET", hypothesis="Unknown route response")
    request("/msg", method="GET", hypothesis="Wrong method response")
    request("/__reset", hypothesis="Reset baseline")
    for body in [{}, {"m": None}, {"m": 3}, {"m": ""}, {"m": "PING"}, {"m": "PING;XX"}, {"m": "PING;a;b"}, {"m": "ALTA;a;b;XX"}, {"m": "UNKNOWN;a;XX"}]:
        request("/msg", body, hypothesis="Input validation order and PING arity")
    for prefix in ["ALTA;", "ALTA;A", "ALTA;B", "CONSULTA;ac-0001", "DEPOSITO;AC-0001;0", "DEPOSITO;AC-0001;-1", "DEPOSITO;AC-0001;1.5", "DEPOSITO;AC-0001;abc", "DEPOSITO;AC-0009;abc", "DEPOSITO;AC-0001;500000", "CONSULTA;AC-0001", "TRANSFER;AC-0001;AC-0002;100001", "TRANSFER;AC-0001;AC-0001;10", "CONSULTA;AC-0001", "ANULA;OP-9999", "ANULA;OP-0001", "ANULA;OP-0001", "CONSULTA;AC-0001", "RETIRO;AC-0001;10", "EXTRACCION;AC-0001;10"]:
        msg(prefix, "Financial boundaries, cancellation effects and command existence")
    request("/__reset", hypothesis="Reset before cancellation sequence", seq="cancel-transfer")
    for prefix in ["ALTA;A", "ALTA;B", "DEPOSITO;AC-0001;200000", "TRANSFER;AC-0001;AC-0002;100000", "ANULA;OP-0002", "CONSULTA;AC-0001", "CONSULTA;AC-0002", "TRANSFER;AC-0002;AC-0001;100001", "TRANSFER;AC-0009;AC-0008;100001", "DEPOSITO;AC-0001;200000"]:
        msg(prefix, "Cancellation must not be assumed to reverse balances; validation precedence")
