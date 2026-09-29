# Manus Intent Port Contract

## Purpose and boundary

`manus/` is **operator-protected** code and configuration. It defines the only accepted hand-off from Manus to the repository: a validated, data-only forecasting intent. Manus cannot use this contract to perform a trading, broker, credential, filesystem, Git, or strategy action.

`runtime/manus/` is reserved for a future agent-writable runtime area and is intentionally ignored by Git. This patch does not create, read, or write that directory.

An intent is a forecast record plus optional strategy **suggestions**. It is not an instruction stream, an execution plan, a file patch, a Git operation, an order, or a connector request.

## Accepted JSON document

The document must be one JSON object with **exactly** these fields. All fields are required; unknown fields are rejected.

| Field | Type and bounds | Contract |
|---|---|---|
| `intent_id` | canonical lowercase UUIDv4 | Immutable application-event id; the forecast bridge persists it as durable provenance. |
| `candidate_id` | identifier, 1–128 chars | Candidate associated with the forecast. |
| `market_id` | identifier, 1–128 chars | Market associated with the forecast. |
| `outcome` | display text, 1–128 chars | Exact outcome label from the prepared candidate packet; spaces and normal punctuation are allowed. |
| `estimated_probability` | JSON number, `0 < value < 1` | Probability estimate; booleans, NaN, and infinity are invalid. |
| `category` | lowercase label, 1–64 chars | Forecast category. |
| `rationale` | text, 1–2,000 chars | Human-readable, non-executable forecast rationale. |
| `edge_class` | lowercase label, 1–64 chars | Classification of the forecast edge. This Patch 3 forecast bridge does not use it for a position or order. |
| `mode` | literal `PAPER` | Explicit paper-only operating mode. No live or real mode exists in this contract. |
| `forecast_disposition` | lowercase label, 1–64 chars | Data-only forecast classification, mapped to the existing `skip_reason` field. `bet` still records only a forecast. |
| `strategy_proposals` | array of 0–10 proposal records | Inert, non-binding strategy suggestions. They are validated then discarded by the forecast bridge. |

A proposal record must contain exactly `proposal_id` and `summary`. A proposal id is a `proposal-` prefixed lowercase identifier up to 64 characters; a summary is non-empty text up to 500 characters.

The machine-readable version of this contract is [`intent.schema.json`](intent.schema.json). The Python validator performs additional safety checks that JSON Schema alone cannot express.

## Explicitly prohibited content

The validator rejects, at every level, fields that express or carry:

- file paths, path traversal, file writes, or file edits;
- commands, shells, executables, scripts, or subprocesses;
- Git branches, commits, pushes, merges, pull requests, or other Git operations;
- real-trading requests, orders, executions, fills, positions, or brokers;
- IBKR / Interactive Brokers order or execution data;
- Pearl or Pearl Connect data; and
- credentials, secrets, tokens, passwords, authorization data, cookies, or keys.

It rejects control characters, absolute filesystem paths, and `../` / `..\` traversal in every string field. Strategy-proposal summaries remain data only: they may discuss trading concepts, including bets, markets, orders, positions, buying, selling, and trading. They are never executed. Summaries are nevertheless rejected when they contain shell syntax, executable commands, credential material, an explicit arbitrary file operation, or an executable/actionable Git operation.

## Validator interface

```python
from manus.intent_validator import IntentValidationError, validate_intent

validated = validate_intent(raw_json, already_applied_intent_ids={"..."})
```

`raw_json` must be a `str`, `bytes`, or `bytearray` containing one JSON document. `already_applied_intent_ids` may be a `set`, `frozenset`, `list`, or `tuple` of previously applied IDs. If the supplied `intent_id` is already present, `validate_intent` raises `IntentValidationError`.

On success, the function returns the parsed JSON object. It does **not** write files, invoke commands, contact services, create Git objects, or take trading actions. On failure, it raises `IntentValidationError` and returns no partially accepted intent.

## PAPER cycle guardian

`python -m manus.paper_cycle_guardian` is the operator-owned bridge between an operator-controlled fixture and an untrusted Manus intent. It supports two data-only, stdout-only operations and one narrow forecast-recording operation:

```text
python -m manus.paper_cycle_guardian prepare --fixture <trusted-fixture.json>
python -m manus.paper_cycle_guardian validate-intent --fixture <trusted-fixture.json> --intent <intent.json> [--already-applied <intent-ids.json>]
python -m manus.paper_cycle_guardian record-forecast --fixture <trusted-fixture.json> --intent <intent.json> [--already-applied <intent-ids.json>]
```

Patch 2 functionality intentionally supports an **offline fixture**; `prepare` and `validate-intent` do not invoke `core/scan.py`, market APIs, broker code, or any execution path. The fixture uses the public `core/scan.py` candidate fields (`market_id`, `question`, `outcomes`, `outcome_prices`, `end_date`, and optional `description`). `fixture.generated_at` and each `end_date` must be ISO-8601 timestamps with an explicit UTC offset; whole seconds, fractional seconds, `Z`, and offsets such as `+00:00` are accepted. They normalize deterministically to UTC `Z` form (`YYYY-MM-DDTHH:MM:SS[.fraction]Z`) before packet and candidate identifiers are derived.

External market `question`, `description`, and outcome labels are **untrusted quoted data**, never instructions. The guardian permits printable Unicode and canonically normalizes CRLF / CR to LF and TAB to a space; it rejects NUL and other inappropriate C0/C1 control characters while retaining strict field length limits. It does not interpret market text as shell, Python, Git, file paths, commands, credentials, or execution requests, and it never executes that text.

`packet_id` and `candidate_id` are deterministic identifiers, **not cryptographic authentication**. A party that changes a packet can recompute its hashes. Therefore both `validate-intent` and `record-forecast` accept no packet input: each loads the original operator-controlled fixture, calls `prepare_packet()` internally, calls `validate_intent`, and binds the intent’s candidate id, market id, and outcome against that freshly reconstructed packet. Trusted fixture provenance is an operator/runtime responsibility; fixtures must not be stored in an agent-writable location. Manus receives the prepared packet for research but cannot supply the authoritative packet used during validation.

### Guarded forecast recording

`record-forecast` is the first controlled mutation in this port. After all fixture and Manus validation completes, it passes only these values to the protected `core.forecast.record_forecast()` function:

- trusted candidate `market_id`;
- exact trusted candidate outcome matching the validated intent;
- validated `estimated_probability`, `category`, and `forecast_disposition` (as the existing `skip_reason`); and
- validated `intent_id` as required `source_intent_id` provenance.

The validated `rationale` is recorded only as the existing data-only forecast `note`. `strategy_proposals` are not passed anywhere and are never interpreted or executed. The guardian does not accept journal paths, forecast arguments, token IDs, bid/ask values, market prices, strategy revisions, `--supersede`, `--confirm-extreme`, ledger arguments, execution options, or broker fields.

The protected forecast function preserves its ordinary public, read-only Polymarket checks for this operation: Gamma market lookup, exact outcome/token mapping, and CLOB bid/ask lookup. Fixture `outcome_prices` are not substituted for the live forecast benchmark. Existing guards remain active, including market closed, unknown outcome, unavailable book, probability range, duplicate open market/outcome, and extreme-disagreement confirmation. The guardian always passes `supersede=False` and `confirm_extreme=False`; an extreme forecast is rejected for explicit operator handling rather than automatically confirmed.

New bridge-created rows contain optional `source_intent_id`, a canonical UUIDv4 equal to `intent_id`. Before public market I/O or writing, `core.forecast.record_forecast()` scans every forecast row and rejects any previously seen `source_intent_id`, regardless of market state, settlement, or supersession. This makes a completed append durable retry detection. The initial implementation retains the forecast book’s existing **single-writer** assumption and adds no multi-process lock or recovery protocol.

The only successful `record-forecast` mutation is one new row in `journal/forecasts.jsonl`, performed by protected `core/forecast.py` after all validation and live market checks. For a provenance-bearing guarded row, the writer preserves the complete current book in a same-directory temporary file, appends the complete JSONL row, flushes and fsyncs it, closes it, and only then atomically replaces the forecast book. An ordinary exception before replacement leaves the original book byte-for-byte unchanged and cleans up the temporary file where possible. This retains the existing **single-writer** assumption; no locking or concurrency mechanism is added. The route does not write `journal/ledger.jsonl`, create a paper position, make an order, call `core/ledger.py`, `core/real.py`, IBKR, Pearl, an authenticated API, a paid API, or `loop.sh`. A rejection or market-data failure before append creates no forecast record. After a successful replacement, `source_intent_id` prevents a retry from creating a second forecast.

## Example valid paper intent

```json
{
  "intent_id": "123e4567-e89b-42d3-a456-426614174000",
  "candidate_id": "cand-20260926-01",
  "market_id": "market:example-001",
  "outcome": "Miami (OH)",
  "estimated_probability": 0.62,
  "category": "macro",
  "rationale": "The outcome is underpriced relative to the evidence.",
  "edge_class": "calibration",
  "mode": "PAPER",
  "forecast_disposition": "no-edge",
  "strategy_proposals": [
    {
      "proposal_id": "proposal-calibration-01",
      "summary": "Require a larger edge before paper bets in thin markets."
    }
  ]
}
```

## Guarded PAPER placement

`record-paper-placement` is the first and only Manus-port operation that may create a **simulated paper-ledger row**. It is deliberately separate from `record-forecast`: an operator-controlled fixture and a persisted, fixture-bound forecast must already exist before a placement is considered.

```text
python -m manus.paper_cycle_guardian record-paper-placement \
  --fixture <trusted-fixture.json> \
  --intent <intent.json> \
  [--already-applied <intent-ids.json>]
```

The command accepts **only** `--fixture`, `--intent`, and optional `--already-applied`. It exposes no option for a stake, ledger or forecast path, forecast ID, packet ID, event ID, token ID, bid, ask, entry price, edge, risk limit, confirmation override, broker, order, live/real mode, Pearl, IBKR, or output path. Internal path and clock values are private test seams, not CLI inputs.

### Binding and validation order

Before any forecast lookup, paper-ledger mutation, Gamma read, or CLOB read, the guardian:

1. loads and reconstructs the operator-controlled fixture packet;
2. applies the existing strict fixture-bound intent validation;
3. requires `mode == "PAPER"` and `forecast_disposition == "bet"`; and
4. recovers `event_id` only from the unique same-fixture candidate whose validated packet candidate already binds the intent. The fixture value must be a strict identifier; `market_id` is never substituted for a missing event ID.

The packet's deterministic `packet_id` is the guarded Manus cycle identifier. Manus cannot provide a forecast ID, packet ID, or event ID independently.

Before any public market I/O, protected core code reads the forecast book and requires **exactly one** row matching `source_intent_id`. The row must be open and not superseded, and its `market_id`, `outcome`, `est_prob`, `category`, `skip_reason == "bet"`, and `note` must exactly match the fixture-bound intent. A missing, duplicate, settled, superseded, or mismatched forecast rejects without a placement.

### Protected paper risk policy

The protected core reads existing `strategy/risk.json` and `config/protected.json`; Manus supplies no stake, price, threshold, or policy choice.

| Control | Guarded Manus PAPER rule |
|---|---|
| Stake | The deterministic `default_stake_usd`, accepted only when `0 < default_stake_usd <= max_stake_usd`; it is never clamped or dynamically sized. |
| Edge | `ask_edge = estimated_probability - live_best_ask`; compare `Decimal(str(value))` input values exactly and require `ask_edge >= max(min_edge, min_edge_book_devig)`. The current configuration is therefore 0.07, but it is not hard-coded. Rounding is only for stored/report fields after the gate passes. |
| Spread | Require both live bid and ask; compare exact `Decimal(str(value))` inputs and require `0 <= best_ask - best_bid <= max_spread`. There is **no** tolerance or wide-book exception, and `min_edge_wide_book` is not used by this guarded route. Rounding is only for stored/report fields after the gate passes. |
| Existing hard controls | Simulated bankroll, open-position cap, protected max stake, probability bounds, duplicate open market/outcome, live market-open state, exact live outcome/token mapping, ask availability, and protected entry-price bounds remain enforced. |
| Resolution time | Parse live Gamma `endDate` as explicit-offset ISO-8601 and require more than `min_minutes_to_resolution`; the fixture time is not used for this live risk check. |
| Cycle caps | Count all guarded rows with the same `source_packet_id`, including settled rows, for `max_new_positions_per_cycle`; count the same cycle plus category for `max_positions_per_category_per_cycle`. Legacy rows without `source_packet_id` are not part of a guarded Manus cycle. |
| Event exposure | Sum matching-event open guarded rows and matching-event legacy rows. Legacy rows without `event_id` are resolved through Gamma's scan-equivalent `/markets?id=` event representation: the protected route requires a non-empty `events` list and uses only `events[0].id`, exactly as `core/scan.py` does. Inability to resolve any open legacy row fails closed. The trusted fixture event ID must also equal the live market event ID. |

The guarded route preserves Phil's paper-fill semantics: public Gamma market data, exact outcome/token mapping, public CLOB book, and a simulated taker fill at the live best ask. It adds no credentials, authenticated market access, paid service, broker call, or real order.

### Provenance, replay, and atomic write

A successful guarded ledger row preserves all legacy required fields and also stores:

- `source_intent_id` — the validated UUIDv4 intent ID;
- `source_forecast_id` — derived from the one matching persisted forecast;
- `source_packet_id` — the reconstructed deterministic packet ID; and
- `event_id` — recovered from the trusted fixture after candidate binding.

The protected row assigns `edge_class = "manus-paper-only"`; it is not selected by Manus. The policy loader requires a safely interpretable `config/protected.json` real allowlist and fails closed before market I/O if that class ever appears in `real.allowed_edge_classes`. The validated research label may be retained as inert `research_edge_class` metadata, but it never selects thresholds, permits a spread exception, changes stake, or affects real eligibility. The guardian contains no executable path to `core.real`, `REAL.md`, Pearl, IBKR, or subprocess-based broker execution, and it never creates a real twin.

Before Gamma/CLOB I/O, the protected function rejects any earlier ledger row with the same `source_intent_id` **or** the same derived `source_forecast_id`, regardless of settlement status. This makes a successful write followed by caller failure safely retry-detectable.

For provenance-bearing guarded rows, the ledger append uses the same single-writer atomic design as the forecast bridge: it preserves original ledger bytes in a same-directory temporary file, appends one complete JSONL row, flushes and fsyncs, closes the temporary file, and only then performs `os.replace()`. An ordinary exception before replacement leaves the original ledger byte-for-byte unchanged, leaves no first-write ledger file, and cleans up the temporary file where possible. No locking or concurrency mechanism is added: the guarded Manus cycle retains the documented **single-writer** limitation, and the legacy runner must not concurrently place positions during such a cycle.
