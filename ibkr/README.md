# Patch 5F-1 — fail-closed IBKR read-only adapter

Status: **strictly read-only infrastructure discovery**. This package can
never submit, modify, replace, or cancel an order, exercise a contract,
move funds, or change broker configuration. There is no LIVE or PAPER
arming capability. Read-only means read-only.

## Operator configuration (local, never committed)

The adapter requires an operator-owned JSON file, resolved from (first
match wins):

1. `PHIL_IBKR_CONFIG` environment variable (a file path),
2. `config/ibkr.local.json` in the repository (gitignored), or
3. `~/.config/phil/ibkr.json`.

Shape:

```json
{
  "expected_account_id": "DU0000000",
  "environment": "PAPER",
  "host": "127.0.0.1",
  "port": 7497,
  "client_id": 19,
  "read_only_timeout_seconds": 10
}
```

Missing file, unknown fields, missing fields, or an unknown environment
string fail closed **before** any connection attempt. Never commit a real
account number, host, port, or credential.

## Interface selection

The official IBKR TWS / IB Gateway Python API (`ibapi`) is used, chosen
over the Client Portal Web API because it supports unattended Windows
operation with a long-lived session, reliable reconnect/recovery, complete
account/position/order/execution visibility, contract qualification, and
the eventual PAPER/LIVE execution paths behind separate hard controls. No
third-party wrapper (ib_insync / ib_async) is introduced. The `ibapi`
import is deferred so importing the package has no side effects and the
cloud test suite never requires the dependency.

The supported operator installation path is the Python package from the
official IBKR TWS API distribution (the downloaded official source,
installed with local pip/install tooling). Do not install the broker API
from PyPI.

## Official Python connection lifecycle (Patch 5F-1a)

The transport uses the official Python API connection sequence exactly:

    client.connect(host, port, clientId)  # creates and starts the reader
    client.run()                          # one bounded daemon thread per
                                          # connected transport processes
                                          # the incoming message queue and
                                          # invokes EWrapper callbacks
    client.disconnect()                   # ends the session and the loop

A successful TCP connect alone is not sufficient: every adapter read waits,
bounded, for the official initial-handshake callback (`nextValidId`) to
arrive through the message-processing path before issuing requests. That
payload is discarded; it is a readiness indication only and never an
order-id authority. Readiness failure disconnects and fails closed with a
bounded diagnostic; there is no reconnect loop and no retry storm.

## Public surface (`ibkr.adapter.ReadonlyIbkrAdapter`)

- `status()` — snapshot: connection, environment, masked account, base
  currency, net liquidation, available funds, position/order counts,
  bounded diagnostic code.
- `account_summary()` — normalized summary for the allowlisted account.
- `positions()` — normalized, deterministically ordered positions.
- `open_orders()` — normalized open orders (never altered).
- `executions()` — normalized recent fills (never altered).
- `lookup_contract(...)` — read-only contract discovery; zero matches →
  `contract-not-found`, ambiguous → `contract-ambiguous`, malformed →
  `invalid-broker-response`. No Polymarket→IBKR mapping is performed and
  no order object is ever constructed.
- `interface`, `close()`, context-manager support.

There is deliberately no placeOrder/submit/cancel/modify/exercise/transfer
method or equivalent, and the mutation-capable official client is held in
a per-instance randomized private attribute of the private transport and
is never returned by any public member.

## Operator CLI

```
python -m ibkr.readonly status
```

Prints compact canonical JSON (sorted keys, no secrets, masked account).
Exit code 1 with a bounded `diagnostic_code` on any fail-closed condition.

## Bounded diagnostics (5E-5a pattern)

Codes: `not-configured`, `connection-unavailable`, `session-unavailable`,
`unexpected-account`, `multiple-accounts`, `environment-ambiguous`,
`unsupported-environment`, `broker-data-unavailable`,
`invalid-broker-response`, `contract-not-found`, `contract-ambiguous`,
`unclassified`.

Classifications: `configuration`, `infrastructure`, `provenance`,
`internal`. Broker exception text is discarded, never logged or persisted.
No retry policy is introduced.

## Architecture separation

This package is NOT imported by `manus.research_transport`,
`manus.paper_runner`, `manus.paper_apply`, `manus.scheduled_paper`, or any
scheduler administration surface, and creates no route from the PAPER
runner to IBKR. The existing IBKR forbidlists in those protected modules
remain correct and unchanged. The planned gate order continues: 5F-1
(read-only boundary) → 5E-6 decision provenance → 5F-2 exact instrument
mapping → later PAPER execution behind separate hard controls.
Real-money execution remains prohibited.
