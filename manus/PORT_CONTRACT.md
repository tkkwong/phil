# Manus Intent Port Contract

## Purpose and boundary

`manus/` is **operator-protected** code and configuration. It defines the only accepted hand-off from Manus to the repository: a validated, data-only forecasting intent. Manus has no authority through this contract to perform an external action.

`runtime/manus/` is reserved for a future agent-writable runtime area and is intentionally ignored by Git. This patch does not create, read, or write that directory.

An intent is a forecast record plus optional strategy **suggestions**. It is not an instruction stream, an execution plan, a file patch, a Git operation, an order, or a connector request.

## Accepted JSON document

The document must be one JSON object with **exactly** these fields. All fields are required; unknown fields are rejected.

| Field | Type and bounds | Contract |
|---|---|---|
| `intent_id` | canonical lowercase UUIDv4 | Unique, immutable id for duplicate protection. |
| `candidate_id` | identifier, 1–128 chars | Candidate associated with the forecast. |
| `market_id` | identifier, 1–128 chars | Market associated with the forecast. |
| `outcome` | display text, 1–128 chars | Exact outcome label from the prepared candidate packet; spaces and normal punctuation are allowed. |
| `estimated_probability` | JSON number, `0 < value < 1` | Probability estimate; booleans, NaN, and infinity are invalid. |
| `category` | lowercase label, 1–64 chars | Forecast category. |
| `rationale` | text, 1–2,000 chars | Human-readable, non-executable forecast rationale. |
| `edge_class` | lowercase label, 1–64 chars | Classification of the forecast edge. |
| `mode` | literal `PAPER` | Explicit paper-only operating mode. No live or real mode exists in this contract. |
| `forecast_disposition` | lowercase label, 1–64 chars | Data-only forecast classification, such as `bet`, `no-edge`, or `market-agrees`. |
| `strategy_proposals` | array of 0–10 proposal records | Inert, non-binding strategy suggestions only. |

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

It rejects control characters, absolute filesystem paths, and `../` / `..\\` traversal in every string field. Strategy-proposal summaries remain data only: they may discuss trading concepts, including bets, markets, orders, positions, buying, selling, and trading. They are never executed. Summaries are nevertheless rejected when they contain shell syntax, executable commands, credential material, an explicit arbitrary file operation, or an executable/actionable Git operation.

## Validator interface

```python
from manus.intent_validator import IntentValidationError, validate_intent

validated = validate_intent(raw_json, already_applied_intent_ids={"..."})
```

`raw_json` must be a `str`, `bytes`, or `bytearray` containing one JSON document. `already_applied_intent_ids` may be a `set`, `frozenset`, `list`, or `tuple` of previously applied IDs. If the supplied `intent_id` is already present, `validate_intent` raises `IntentValidationError`.

On success, the function returns the parsed JSON object. It does **not** write files, invoke commands, contact services, create Git objects, or take trading actions. On failure, it raises `IntentValidationError` and returns no partially accepted intent.

## PAPER cycle guardian

`python -m manus.paper_cycle_guardian` is the operator-owned, data-only bridge between an operator-controlled fixture and an untrusted Manus intent. It supports exactly two **stdout-only** operations:

```text
python -m manus.paper_cycle_guardian prepare --fixture <trusted-fixture.json>
python -m manus.paper_cycle_guardian validate-intent --fixture <trusted-fixture.json> --intent <intent.json> [--already-applied <intent-ids.json>]
```

Patch 2 intentionally supports an **offline fixture** only; it does not invoke `core/scan.py`, network APIs, broker code, or any execution path. The fixture uses the public `core/scan.py` candidate fields (`market_id`, `question`, `outcomes`, `outcome_prices`, `end_date`, and optional `description`). `fixture.generated_at` and each `end_date` must be ISO-8601 timestamps with an explicit UTC offset; whole seconds, fractional seconds, `Z`, and offsets such as `+00:00` are accepted. They normalize deterministically to UTC `Z` form (`YYYY-MM-DDTHH:MM:SS[.fraction]Z`) before packet and candidate identifiers are derived. `prepare` emits a deterministic PAPER packet with `packet_version`, `packet_id`, `generated_at`, `mode`, and sorted candidates. Each candidate receives a deterministic guardian-generated `candidate_id` derived from its trusted emitted data.

External market `question`, `description`, and outcome labels are **untrusted quoted data**, never instructions. The guardian permits printable Unicode and canonically normalizes CRLF / CR to LF and TAB to a space; it rejects NUL and other inappropriate C0/C1 control characters while retaining strict field length limits. It does not interpret market text as shell, Python, Git, file paths, commands, credentials, or execution requests, and it never executes that text.

`packet_id` and `candidate_id` are deterministic identifiers, **not cryptographic authentication**. A party that changes a packet can recompute its hashes. Therefore `validate-intent` accepts no packet input: it loads the original operator-controlled fixture, calls `prepare_packet()` internally, calls `validate_intent`, and binds the intent’s candidate id, market id, and outcome against that freshly reconstructed packet. Trusted fixture provenance is an operator/runtime responsibility; fixtures must not be stored in an agent-writable location. Manus receives the prepared packet for research but cannot supply the authoritative packet used during validation.

The guardian reads only explicitly supplied input paths and prints successful JSON to stdout. It has no `--output` option and performs **no filesystem writes**. An operator may redirect stdout outside the guardian if a file is needed. A successful result is data only: it neither writes a forecast or ledger entry nor persists duplicate ids, executes rationale/proposals, or creates a paper position. The CLI rejects options that suggest real, live-trading, execution, ordering, IBKR, or Pearl behavior.

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
