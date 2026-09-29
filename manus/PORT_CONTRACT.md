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
