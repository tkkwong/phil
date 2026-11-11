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

## Patch 5A: research-only Manus transport

Patch 5A adds the operator-owned `python -m manus.research_transport --fixture <trusted-fixture.json> --candidate-id <candidate-id> [--dry-run]` bridge. It automates only the bounded research hand-off:

```text
trusted frozen fixture
  -> reconstruct trusted packet and select one candidate
  -> standalone connector-free Manus API v2 research task
  -> structured research-judgment extraction
  -> locally assembled trusted final intent
  -> existing strict fixture-bound validation
  -> fixed external validated-intent staging
  -> STOP
```

It does **not** call `record-forecast`, `record-paper-placement`, `core.forecast`, `core.ledger`, `core.real`, Pearl, IBKR, or any broker. It does not touch forecast or ledger journals, strategy, repository files, Git, or a runtime output path supplied by Manus or the CLI. The first live test remains manually initiated by an operator after review and merge; live Manus tasks consume Manus service credits when eventually run.

### Task isolation

The transport uses the fixed production API base `https://api.manus.ai/v2` and only the narrow v2 task endpoints required for one asynchronous task: `task.create`, `task.listMessages`, `task.detail`, and, after a timeout or waiting state, best-effort `task.stop`. It never calls `task.sendMessage`, `task.confirmAction`, connectors, projects, files, browser, agents, webhooks, or any GitHub/third-party connector endpoint.

`task.create` is created as a private standalone task with `interactive_mode=false`, `hide_in_task_list=true`, stable `agent_profile="standard"`, and an explicit `message.connectors: []`. No `project_id`, `task_references`, caller attachment references/files, connector IDs, or browser context are supplied. The empty connector array is deliberate: omitting it could inherit account-default connectors. Manus may internally materialize long prompt text as a cloud-side text attachment containing only the bounded prompt/candidate material; it is not caller-provided attachment authority, grants no local filesystem, GitHub, connector, broker, or credential authority, receives no client attachment handling/download logic, and its URL is neither printed nor persisted. A waiting state fails closed; the runner sends neither a follow-up nor an action confirmation. The structured-output schema is only an extraction shape; the existing local validator remains the security and fixture-binding boundary.

### Manus Skill inheritance limitation

Patch 5A explicitly disables connector inheritance for `task.create` with `message.connectors: []`. It supplies no `project_id`, `task_references`, browser context, attachments, GitHub connector access, or local filesystem access.

Current documented Manus API v2 does **not** provide a per-task switch that guarantees zero enabled Skills. The transport currently omits both `message.enable_skills` and `message.force_skills`; documented v2 behavior is that an omitted or empty `message.enable_skills` array may load the account-default enabled Skills. There is no documented `clear_skills`, `disable_skills`, or `use_default_skills=false` equivalent for `task.create`. Patch 5A therefore does **not** claim complete Manus-cloud capability isolation.

Patch 5A does not rely on Manus Skills for correctness, validation, market identity, candidate binding, execution, credentials, file mutation, or broker access. The authoritative security boundary is local: Manus receives no Phil filesystem authority, no Windows Credential Manager authority, no journal-mutation authority, and no IBKR, Pearl, or broker credentials. Manus output remains data only; the strict fixture-bound validator remains authoritative; and forecast recording and PAPER placement remain separate operator-controlled actions.

For a live research task, the operator should disable unneeded account-default Skills where practical. This is an account-level operational precaution, **not** a per-task API guarantee. If Manus later documents a per-task zero-Skills control, Patch 5A should be updated to use it and to add a regression test.

### Credential and staging boundary

A live Windows run reads only the `phil-manus-api` Generic Credential from Windows Credential Manager, requires its exposed username to be `MANUS_API_KEY`, keeps it in process memory solely for the `x-manus-api-key` HTTPS header, frees the returned credential memory with `CredFree`, and never prints, returns, persists, or accepts an API key. There is no environment, file, registry, package, subprocess, or PowerShell credential fallback. Non-Windows live runs fail closed.

After successful local fixture-bound validation, and only then, the runner stages two canonical JSON files outside the repository at the fixed `%LOCALAPPDATA%\phil-manus\staging\<packet_id>\<intent_id>\` path: `validated-intent.json` and inert `run-meta.json`. `run-meta.json` contains only API version, task/packet/candidate/intent IDs, fixture SHA-256, timestamps, agent profile, and validation version. It contains no API key, headers, cookies, connector data, hidden reasoning, or raw Manus output. A pre-existing intent path rejects without overwrite. Each file is flushed, fsynced, closed, and atomically replaced in a same-directory temporary file; the completed intent directory is published only after both files exist. Ordinary failures clean temporary staging data where possible.

Patch 5A requires `strategy_proposals == []` even though the general intent contract permits inert suggestions. It also accepts only documented forecast disposition labels as research metadata. `--dry-run` validates the fixture, reconstructs and selects the packet candidate, constructs the exact bounded request, verifies fixed staging-root calculation, and prints a safe summary; it reads no credential, calls no API, writes no staging artifact, and mutates no journal.

### Patch 5A.1: trusted intent assembly and pre-create paid-task reservations

Patch 5A.1 changes the Manus structured-output contract from a complete intent to six **research judgments only**: `outcome`, `estimated_probability`, `category`, `rationale`, `edge_class`, and `forecast_disposition`. The output schema limits `outcome` to the exact selected trusted candidate outcomes. It limits `forecast_disposition` to the research-appropriate documented labels `bet`, `no-edge`, `market-agrees`, `ambiguous-resolution`, `architecture-mismatch`, `outside-view-veto`, and `unvalidated-method`. The last four are existing research-review labels; local category/event/risk policy gates are deliberately excluded. The task instruction requires lowercase Phil labels for `category` and `edge_class`; invalid values are not normalized, repaired, or otherwise salvaged.

Only protected local code creates the complete intent after a successful meaningful structured result. It generates a new canonical lowercase UUIDv4 `intent_id` using the Python standard library, injects the exact trusted `candidate_id` and `market_id`, sets `mode` to literal `PAPER`, and sets `strategy_proposals` to literal `[]`. It then passes that assembled document unchanged through the existing strict fixture-bound validator. Manus never supplies or influences those authority-bearing fields. A locally invalid research field rejects with no staged intent and no raw Manus result persisted.

Before **any** `task.create` request, the transport reconstructs the trusted packet, selects the candidate, constructs the exact credential-free payload, computes `request_sha256` from its canonical JSON serialization, and atomically creates a candidate-bound pre-create reservation at `%LOCALAPPDATA%\phil-manus\staging\pending\<deterministic-trusted-key>.json`. The key is derived only from trusted `packet_id`, `candidate_id`, `market_id`, and `fixture_sha256`; deliberately omitting the request hash from the filename causes a changed prompt, schema, or request policy to collide and fail closed rather than silently create another paid task.

Each reservation contains only `task_id` (initially `null`), `packet_id`, `candidate_id`, `market_id`, `fixture_sha256`, `request_sha256`, `transport_schema_version`, `created_at`, and a safe state. The fingerprint covers the entire exact `task.create` JSON body: fixed instructions/prompt, quoted selected-candidate data, connector policy, task privacy/settings, agent profile, and structured-output schema. It excludes the API key and all HTTP headers. It never contains the API key, headers, cookies, connectors, raw output, rationale, or hidden reasoning.

The reservation is written with same-directory temporary-file creation, complete canonical JSON, flush, `fsync`, close, and `os.replace`; failure to create it means **no** `task.create` request is sent. A successful create response updates the same reservation with the validated task ID before polling. The explicit states are `reserved`, `ambiguous-create`, `created`, `polling`, `rejected-local-validation`, `task-error`, `waiting`, `timeout`, `unknown`, and `completed`.

`task.create` remains non-retryable. If its response is ambiguous, the durable reservation becomes `ambiguous-create`, and the error reports `phase=task.create`, `request_sha256`, the safe reservation key, and that reconciliation is required; it never claims whether a task was created. If a task ID is known but its reservation update fails, polling stops immediately and the error safely reports that task ID and requires reconciliation. A subsequent normal invocation sees either no-task reservation state and makes **zero** POSTs, or a valid known task ID and resumes only that exact task. There is no `--force`, `--retry-create`, `--ignore-receipt`, automatic create override, `task.sendMessage`, or `task.confirmAction` path.

Request-hash or transport-version drift for an existing reservation fails closed before credential access or network I/O. The operator reconciliation process is intentionally manual: retain the reservation as evidence, inspect/reconcile the known task in the service if available, then begin an intentional new frozen cycle only after resolving whether a paid task exists. This patch never deletes or recreates a reservation automatically.

After a known task ID exists, only read-only `task.listMessages` and `task.detail` polling calls receive bounded retries for transient network, timeout, HTTP 408, HTTP 429, or HTTP 5xx failures within the existing overall deadline. Patch 5A.2 adds one narrower protected exception: for a task ID created in the **same invocation**, before either polling endpoint has returned data, a `404` with `error_code=not_found` is retried for at most `POST_CREATE_VISIBILITY_GRACE_SECONDS = 60` seconds (and no more than eight visibility retries), still bounded by the overall polling deadline. This acknowledges only the brief post-create visibility race; it does not make generic 404 responses, old/resumed tasks, unrelated GETs, or `task.create` retryable. The first successful response from either endpoint marks the task visible for that invocation, so a later 404 fails closed.

Waiting, task error, timeout, and unknown conditions retain/update the reservation and fail closed. On validated success, complete intent staging is durable before the reservation becomes `completed`; `run-meta.json` retains safe `request_sha256`, `transport_schema_version`, `task_origin`, `task_created_this_invocation`, `requested_agent_profile`, and, where supplied by `task.detail`, `resolved_agent_profile`. `agent_profile` remains a backward-compatible alias for the requested profile only; a service-resolved profile such as `manus-1.6` is recorded separately and never selects routing or security policy. Safe invocation output similarly includes `task_id`, `task_origin` (`created` or `resumed`), and `task_created_this_invocation`, so a resumed reservation never implies a new `task.create` occurred. No raw task messages, server-side attachment objects, or attachment URLs are staged or printed. Patch 5A.2 still stops before forecast recording or PAPER placement.

## Patch 5C: cross-process mutual exclusion

Patch 5C adds a standard-library, OS-held lock layer for the fixed external Manus working area. In production, locks live only under:

```text
%LOCALAPPDATA%\phil-manus\locks\
```

No CLI accepts a lock path, lock timeout, lock name, lock-break, or stale-lock override. Lock files contain only best-effort diagnostic metadata (`pid`, host, acquisition time, logical purpose, and logical name). Metadata is **not authority**: ownership is the held OS file lock (`fcntl.flock` on POSIX; `msvcrt.locking` on Windows), and process exit releases it. A malformed, stale, or overwritten metadata file never authorizes a second holder or lock break.

The fixed logical locks are:

| Lock | Scope | Behavior |
|---|---|---|
| `cycle` | Reserved fixed future-cycle boundary | Available only to local operator code; Patch 5C adds no scheduler, service, or cycle runner. |
| `request-<request_sha256>` | One exact canonical credential-free `task.create` request | `research_transport` fails cleanly before credential lookup or POST when held by another process. |
| `application-<intent_id>` | One canonical staged PAPER intent | `paper_apply` waits only for a fixed 30-second bound, then fails without mutation if still unavailable. |
| `journal-writer` | Manus receipt reconciliation plus guarded forecast/placement calls | `paper_apply` takes it after the application lock, waits only for the same fixed bound, and leaves legacy writers outside this new contract. |

Global order is `request` for research and `application -> journal-writer` for application. No path takes these locks in reverse order. Locks span the full protected critical sections: reservation read/write plus the single POST/poll/stage lifecycle for research; and receipt read/transitions, journal reconciliation, and guardian forecast/placement calls for application.

Before any non-retryable `task.create`, the candidate-bound reservation is atomically persisted first as `reserved`, then as durable `creating`. If a process dies or loses the response after request initiation but before a `task_id` is recorded, a rerun sees `creating` without a task ID and requires operator reconciliation; it never creates another task. The request lock prevents a concurrent invocation from reading that intermediate state while the original invocation remains alive.

## Patch 5B: manual staged PAPER application

Patch 5B adds one manual operator bridge:

```text
fixed validated staging
  -> reconstruct original trusted fixture packet
  -> revalidate staging provenance and intent binding
  -> existing guarded forecast recording
  -> exact bet only: existing guarded simulated PAPER placement
  -> durable external application receipt
  -> STOP
```

The production CLI is exactly:

```text
python -m manus.paper_apply --fixture <trusted-fixture.json> --intent-id <canonical-uuidv4> [--dry-run]
```

Only the fixture is a caller-supplied path. The bridge derives the staged file locations from the freshly reconstructed packet ID and canonical lowercase UUIDv4 intent ID:

```text
%LOCALAPPDATA%\phil-manus\staging\<packet_id>\<intent_id>\validated-intent.json
%LOCALAPPDATA%\phil-manus\staging\<packet_id>\<intent_id>\run-meta.json
```

The external application receipt has one fixed non-caller-selectable location:

```text
%LOCALAPPDATA%\phil-manus\staging\apply\<packet_id>\<intent_id>.json
```

No production option accepts a staged directory, output location, journal path, stake, price, token, event, edge, spread, risk control, forecast ID, placement ID, real/live/broker setting, retry, force, override, shell, or command. Test-only underscore seams may inject external roots or journal locations; they are not CLI inputs.

### Provenance before mutation

Before reading a journal, public market data, or mutating anything, `paper_apply`:

1. reads the exact trusted fixture bytes and computes its SHA-256;
2. reconstructs the fixture packet using existing `prepare_packet()` logic;
3. derives the fixed staged paths from that packet ID and the canonical CLI intent ID;
4. parses `validated-intent.json` and `run-meta.json` with the existing duplicate-key-safe parser;
5. requires exact packet ID, intent ID, candidate ID, fixture SHA-256, transport schema version, and validation version consistency;
6. requires literal `mode == "PAPER"`, literal `strategy_proposals == []`, and exact trusted candidate, market, and outcome binding; and
7. invokes existing strict `validate_candidate_intent()` again.

The current protected `TRANSPORT_REQUEST_SCHEMA_VERSION` and `VALIDATION_VERSION` constants are imported rather than copied. A version mismatch fails closed; this bridge does not silently reinterpret old staging. Staging is evidence, not authority.

`--dry-run` performs all of the preceding validation and emits only a safe plan (`record-forecast-only` or `record-forecast-then-guarded-paper-placement`). It makes no journal read/write, public market call, Manus call, credential lookup, application receipt write, forecast call, or placement call.

### Application receipt and manual recovery

The receipt contains only the application version/state, packet/intent/candidate/market IDs, fixture and exact-intent SHA-256 digests, disposition, known forecast/placement IDs, and timestamps. It never stores credentials, API keys, headers, cookies, raw market/API data, raw Manus messages, or rationale.

The states are `prepared`, `forecast-pending`, `forecast-recorded`, `placement-pending`, `completed-no-placement`, `completed-placement`, `forecast-rejected`, `placement-rejected`, and `error`. `prepared` is atomically persisted before the first journal mutation with a same-directory temporary file, complete canonical JSON, flush, `fsync`, close, and `os.replace`; if that write fails, no forecast call occurs.

The receipt is orchestration state only. Forecast and PAPER ledger journals are authoritative. A rerun reconciles against their immutable provenance before taking any action:

- a forecast recovery requires exactly one `source_intent_id` row whose market, outcome, probability, category, disposition, and rationale exactly match the staged intent;
- a PAPER placement recovery requires exactly one compatible row whose `source_intent_id`, `source_forecast_id`, `source_packet_id`, market, outcome, probability, category, rationale, and fixed `manus-paper-only` classification bind to that forecast/packet/intent;
- duplicate or mismatched journal rows fail closed; and
- a completed receipt performs zero new journal mutations only after its authoritative journal state reconciles exactly.

A crash or receipt-write failure after a forecast/placement mutation leaves a `*-pending` receipt. A rerun recovers the one existing provenance row and only repairs the receipt; it never attempts another forecast or placement. Terminal `forecast-rejected`, `placement-rejected`, and `error` receipts do not automatically retry. A guarded PAPER placement rejection retains its recorded forecast but creates no ledger row and is terminal until a new research cycle or future explicit reconciliation.

### Forecast and placement boundaries

Every valid staged intent calls existing `record_candidate_forecast()` through the guardian. The bridge does not call `core.forecast` directly and does not duplicate forecast semantics or policy. `source_intent_id` remains the protected replay guard.

Only exact `forecast_disposition == "bet"` can then call existing `record_candidate_paper_placement()` through the guardian. Any non-bet disposition records the forecast, persists `completed-no-placement`, and stops before inspecting placement policy or calling placement. `bet` invokes no new risk logic: the existing protected Patch 4 stake, edge, spread, timing, event, exposure, cycle, cash, token, and real-eligibility controls remain authoritative. A rejection persists `placement-rejected` with zero manufactured ledger row.

Patch 5B remains manual. Patch 5C serializes simultaneous `paper_apply` processes for the same intent and their guarded Manus journal path, but it does **not** extend this locking contract to legacy forecast/ledger writers; the legacy runner must not mutate forecast/ledger journals concurrently. Patch 5C adds no scheduler, service, loop, task, Git automation, Manus API behavior, Credential Manager use, broker, real execution, Pearl, or IBKR route. The only possible position mutation remains the existing guarded simulated PAPER ledger function.


## Patch 5D: bounded manual PAPER cycle runner

`manus.paper_runner` composes existing protected seams for **one manual PAPER cycle**. It does not change `core.scan` filtering/query/pagination/order/deduplication, research request identity, credential handling, Manus HTTP behavior, reservation state, intent validation, forecast mutation, PAPER placement rules, or application-receipt behavior. It consumes only the scanner's protected bounded provider-metadata API controls (`include_provider_metadata=True`, `max_candidates=20`).

### Fixed limits and state model

The runner hard-codes these limits; no CLI option can increase them:

| Limit | Value |
|---|---:|
| Selected candidates per cycle | 1 |
| New Manus tasks per cycle | 1 |
| Forecast applications per cycle | 1 |
| `paper_apply` application initiations per cycle | 1 |

The explicit cycle states are `prepared`, `scanned`, `selected`, `research-pending`, `research-completed`, `application-pending`, `completed`, `completed-no-candidate`, and `failed-terminal`. `research-pending` and `application-pending` are nonterminal. A budget-zero authorization-needed result stays `research-pending`; it is not a completed cycle and does not permit candidate replacement. A guarded PAPER placement rejection is terminal for that cycle and never selects candidate #2.

The counter for a new Manus task is incremented only from the existing transport's truthful `task_created_this_invocation` result. The logical application counter is persisted before the first `paper_apply` call and remains one while a later invocation reconciles the same fixed receipt/journal provenance. `paper_apply`'s durable `application_state`, not a transient placement-status string, is the sole terminal authority: `completed-no-placement` and `completed-placement` complete the cycle; `placement-rejected`, `forecast-rejected`, and `error` end it as `failed-terminal`; unknown or nonterminal application states leave it pending and fail closed.

### Budget mapping and durable-state restriction

`--manus-task-budget` accepts only `0` or `1`; default is `0`. It maps directly and only for the present Python call:

```python
research_transport.run(..., allow_new_task=(current_budget == 1))
```

The runner never derives `allow_new_task=True` from cycle state, advisory soft credit metadata, a prior command line, a transport reservation, task metadata, or staging metadata. The flag is not persisted. `--manus-task-budget=1` additionally requires `--manus-soft-credit-ceiling` from 1 through 100. The ceiling is advisory operator metadata only and is not represented as a Manus provider hard cap.

### Locking and persistent files

Non-dry cycles obtain `paper_locks.acquire_cycle_lock()` first and hold it across the complete invocation. The only valid nesting is `cycle -> request` in `research_transport`, or `cycle -> application -> journal-writer` in `paper_apply`. The lock's OS ownership is authoritative; metadata is diagnostic only. Persistent runner files are fixed under `%LOCALAPPDATA%\phil-manus\runner\`, not the repository or `runtime/manus/`, and never accept caller paths. Atomic writes use temporary file, complete JSON, flush, `fsync`, close, and `os.replace`.

The runner's files are same-Windows-identity operational state, **not** a hard security boundary against arbitrary same-identity processes. It also does not extend the locking contract to legacy `loop.sh` or legacy journal writers; operators must not run those mutation paths concurrently with a guarded manual runner cycle.

### Bounded provider-metadata selection

The runner has no text classifier and treats question, description, outcome, provider tag label, provider tag slug, and `category` as untrusted data. It calls `core.scan.scan_candidates(include_provider_metadata=True, max_candidates=20)` and only considers the protected normalized `provider_metadata` returned with each retained scanner record. A candidate is eligible only when its exact numeric provider tag IDs contain one of `"1"`, `"21"`, or `"64"` and contain none of `"2"`, `"1597"`, `"101206"`, `"101252"`, `"104743"`, `"100265"`, `"126"`, `"100285"`, `"102305"`, `"104010"`, `"104039"`, or `"104608"`. The metadata envelope must have literal `market_tags_status == "ok"` and bounded normalized tag objects; unavailable, invalid, malformed, empty, unknown-only, and mixed allowed/excluded envelopes are ineligible. The first eligible scanner record is selected without ranking or reordering.

The guardian fixture remains restricted to the public immutable `SCAN_SOURCE_FIELDS` contract and never contains provider metadata. The runner persists a separate immutable `{market_id, provider_metadata}` evidence envelope plus its canonical SHA-256 in its fixed external cycle state. Any mismatch, malformed evidence, fixture-only crash window, or missing frozen evidence fails closed without a rescan, replacement, or candidate substitution. This Patch is not an unattended-production authorization.


## Patch 5E-1: disabled-by-default scheduled PAPER wrapper

`manus.scheduled_paper` is a small **one-invocation entrypoint**, not a
scheduler installer. Patch 5E-1 does not install, configure, activate, or claim
any Windows Task Scheduler task, `schtasks` command, COM task, PowerShell
schedule, service, daemon, timer, cron job, startup item, recurring loop, or
retry loop. Windows task registration remains deferred to Patch 5E-2.

The wrapper exposes only normal invocation and `--status`. It has no production
option to enable/disable itself, choose a root/path, configure a schedule,
retry, set a budget/credit ceiling, or select a live/real/broker route. The
fixed opt-in document is local and outside the repository:

```text
%LOCALAPPDATA%\phil-manus\scheduler\enabled.json
{"scheduler_version":"scheduled-paper/v1","enabled":true}
```

A missing marker or exact `enabled: false` is disabled and invokes no runner,
creates no directory/state, and performs no cycle work. The marker parser is
bounded and duplicate-key-safe; it accepts only that exact two-field versioned
document. Invalid JSON, unknown/missing fields, invalid types, oversized input,
symlink, junction, or other detectable reparse-point indirection fails closed
before `paper_runner` is called. `--status` reads only this marker and never
invokes the runner or creates local scheduler state.

Marker input is read at most 1,025 bytes (the 1,024-byte parsing limit plus
one overflow byte) before it is parsed. Runner authority-file reads likewise
fail closed when either the file or its immediate fixed cycle-directory parent
is an ordinary symlink or detectable Windows junction/reparse point.

When the exact local marker enables a call, authority is permanently fixed to:

```python
paper_runner.run(
    dry_run=False,
    manus_task_budget=0,
    manus_soft_credit_ceiling=None,
)
```

There is no code path to request a new Manus task or infer such authority from
prior state. Existing known-task/validated-staging reconciliation can remain
available at zero budget through the protected runner and transport. A fresh
`research-pending` result requiring current-invocation authorization is safely
reported and stopped. A separate manual runner invocation remains the only path
that can explicitly receive a budget of one and its required advisory ceiling.

An enabled call atomically replaces only fixed
`%LOCALAPPDATA%\phil-manus\scheduler\last-run.json`, using a same-directory
temporary file, flush, `fsync`, close, and `os.replace`. That result is a
bounded whitelist of scheduler timestamp/outcome; cycle, market, task, intent,
application, forecast, and placement identifiers/states; zero new-task count;
application/forecast/placement counters; and safe reason. It excludes raw
runner output, prompt text, market prose, rationale, secrets, exception text,
and tracebacks. A runner failure, unexpected exception, malformed result, or
reported nonzero new-task count fails closed with only a generic safe result;
there is no retry or fallback invocation.

The wrapper does not introduce a second full-cycle lock. Overlap handling is
owned by `paper_runner`'s existing outer cycle lock: a winner enters the normal
protected cycle and a loser fails closed without retry. The wrapper has no
direct scanner, transport, credential, application, forecast, placement, risk,
broker, wallet, IBKR, Pearl, or real-execution route. Detectable reparse-point
checks are defensive fixed-path validation, not a hard boundary against another
process under the same Windows identity. Legacy `loop.sh` and legacy journal
writers still do not honor the guarded Manus lock contract and must not run
concurrently with guarded PAPER work.


## Patch 5E-2: fixed Windows scheduler operator controls

`manus.scheduler_admin` is the only Windows registration/operator layer. It
exposes exactly `status`, `install`, `enable`, `disable`, and `uninstall` and
accepts no configuration argument. It uses one fixed task name, `Phil Manus
PAPER Hourly`, one fixed `TimeTrigger` at the next local top-of-hour with
indefinite hourly `PT1H` repetition, and the
repository-root `scheduled_paper_task.py` launcher. The launcher calls only
`scheduled_paper.main([])` after changing to its own repository root.

The operator layer invokes only the exact absolute
`%SystemRoot%\System32\schtasks.exe` process with argument arrays, bounded
timeout, `stdin=subprocess.DEVNULL`, captured raw output, and no shell. It
builds a bounded temporary stdlib XML definition, flushes, `fsync`s, closes,
checks it for fixed-root reparse/symlink indirection, invokes `/Create` once,
and removes it in `finally`. The XML has exactly one action—the exact validated
Python executable and launcher path—and one InteractiveToken principal for the
current validated `USERDOMAIN\USERNAME` identity at `LeastPrivilege`. It has no
stored password, password prompt, credential lookup, SYSTEM/service account, or
highest-privilege mode. The task can run only while that user remains logged in
to an existing interactive session.

Install atomically disarms the existing sole marker **before any later
Scheduler, launcher, identity, Python, XML, or task-definition validation or
action**. It does not run Phil and never automatically re-enables stale state.
Only a successful fixed task query confirms `installed` and permits enable to
arm `enabled:true`; a nonzero query is the exact bounded `not-confirmed` state,
not a claim that the task is absent. Enable does not run the task or invoke the
wrapper, runner, Manus, scan, or research.
Disable is an admission-control kill switch for **future** wrapper entry only;
it neither queries nor changes task installation state and never terminates an
already-running protected cycle or deletes the task. Uninstall likewise
disarms before subsequent utility validation and delete action. A failed create
or delete leaves the marker false without retry.

The unattended route remains permanently budget zero. Explicit budget one
remains manual `paper_runner` authority only. There is no run-now command and
no real, IBKR, Pearl, broker, wallet, or credential route. The fixed XML sets
`MultipleInstancesPolicy=IgnoreNew`, `AllowStartOnDemand=false`,
`AllowHardTerminate=false`, `ExecutionTimeLimit=PT0S`, and explicit
`DisallowStartIfOnBatteries=false` / `StopIfGoingOnBatteries=false`: it may
start while the logged-in computer is on battery and is not stopped merely by a
switch to battery. It never wakes the computer, catches up missed executions,
or creates scheduler retry behavior. Fixed-path/reparse checks and locks are
same-Windows-identity operational controls, not a hard barrier against
arbitrary processes under that identity; legacy `loop.sh` must not run
concurrently with guarded PAPER work.

## Patch 5F-1: fail-closed IBKR read-only adapter

`ibkr/` is a strictly read-only infrastructure discovery boundary. It can never submit, modify, replace, or cancel an order, exercise a contract, move funds, or change broker configuration, and it has no LIVE or PAPER arming capability. Read-only means read-only. The adapter uses the official IBKR TWS / IB Gateway Python API (`ibapi`), chosen over the Client Portal Web API for unattended Windows operation with a long-lived session, reliable reconnect recovery, complete account, position, order, execution, and contract visibility, and the eventual PAPER execution paths behind separate hard controls. No third-party wrapper is introduced. The `ibapi` import is deferred to connect time so importing the package performs no work and the cloud test suite never requires the dependency.

### Public surface and mutation impossibility

The only public surface is `ibkr.adapter.ReadonlyIbkrAdapter`: `status`, `account_summary`, `positions`, `open_orders`, `executions`, `lookup_contract`, `interface`, `close`, and context-manager support. The class exposes no order mutating method of any kind, the mutation capable official client is held only in a name mangled private attribute of the private transport and is never returned by any public member, and a regression test pins the public surface to exactly that closed set. `lookup_contract` performs read-only contract discovery only: zero matches raise `contract-not-found`, ambiguous matches raise `contract-ambiguous`, malformed responses raise `invalid-broker-response`, and no Polymarket to IBKR mapping is performed and no order object is ever constructed.

### Operator configuration and fail-closed behavior

Configuration is operator owned and never committed. It resolves from `PHIL_IBKR_CONFIG`, then `config/ibkr.local.json` (gitignored), then `~/.config/phil/ibkr.json`, and requires `expected_account_id`, `environment` (`PAPER` or `LIVE`), `host`, `port`, and `client_id`. Missing, malformed, unknown field, missing field, ambiguous environment, or invalid numeric configurations fail closed before any connection attempt. Every read path verifies the single connected account against the allowlist and fails closed on mismatch (`unexpected-account`), ambiguity (`multiple-accounts`), or an empty account list (`session-unavailable`), closing the session first. Infrastructure and configuration failures surface as bounded diagnostic codes; broker exception text is discarded, never logged or persisted, and account identifiers are always masked (last four characters). No retry policy is introduced.

### Diagnostics and CLI

Diagnostics follow the Patch 5E-5a closed vocabulary: `not-configured`, `connection-unavailable`, `session-unavailable`, `unexpected-account`, `multiple-accounts`, `environment-ambiguous`, `unsupported-environment`, `broker-data-unavailable`, `invalid-broker-response`, `contract-not-found`, `contract-ambiguous`, `unclassified`, classified as `configuration`, `infrastructure`, `provenance`, or `internal`. `python -m ibkr.readonly status` prints compact canonical JSON with masked accounts, rejects mutation like options, and exits 1 with a bounded code on any fail closed condition. It creates no files and touches no journal or PAPER runtime state.

### Architecture separation

`ibkr/` is not imported by `manus.research_transport`, `manus.paper_runner`, `manus.paper_apply`, `manus.scheduled_paper`, or any scheduler administration surface, and it creates no route from the PAPER runner to IBKR. The existing IBKR forbidlists in those protected modules remain correct and unchanged. The planned gate order continues: this read-only boundary, then 5E-6 decision provenance, then 5F-2 exact instrument mapping, then later PAPER execution behind separate hard controls. Real money execution remains prohibited.

### Patch 5F-1a: official Python transport lifecycle

Real PAPER smoke against the operator's installed official API (ibapi 10.50.2) proved that the Python `EClient` surface is `connect(host, port, clientId)`, `run()`, `disconnect()`, `isConnected()` and has no `eConnect`; Patch 5F-1's initial `eConnect` call was a lifecycle defect copied from the C++/Java style API naming. The transport now uses the official Python sequence exactly: `connect` (which creates and starts the reader), one bounded daemon thread running `client.run()` per connected transport to process the incoming message queue and invoke wrapper callbacks, and `disconnect()` (which ends the loop; the thread join is bounded). A successful TCP connect alone is not sufficient: every read waits, bounded, for the official initial-handshake callback `nextValidId` delivered through the message-processing path; its payload is discarded (readiness indication only, never an order id authority). Readiness timeout, connect failure, and run-loop death all disconnect and fail closed with the existing bounded diagnostics; no reconnect loop and no retry storm exist. Offline regressions model the real Python client shape (connect/run/disconnect/isConnected, deliberately no eConnect; callbacks occur only through the run path) so this defect cannot recur.

### Patch 5E-6: decision provenance and offline shadow/replay

``manus.decision_provenance`` is the typed, versioned, append-only decision
audit boundary for the guarded PAPER workflow. Storage is one fixed JSONL file
under the existing external runtime root
(``%LOCALAPPDATA%\phil-manus\provenance\decision_provenance.jsonl``), never
inside the repository and never in the protected ``journal/`` files. The
schema is ``decision-provenance/v1`` and the frozen-input schema is
``decision-frozen-input/v1``.

- **Single writer / single owner**: only ``manus.paper_apply`` (the existing
  final decision-transition owner) appends operational provenance, under the
  fixed ``provenance-writer`` lock from ``manus.paper_locks``. The inspector
  and every replay/shadow path are strictly read-only.
- **Deterministic identity**: ``decision_id`` is the SHA-256 of a canonical
  binding of execution mode, stage, phase, action, cycle identity
  (``packet_id``), intent/market/event identity, and the frozen-input hash.
  A given frozen decision always produces the same ``decision_id``.
- **Idempotency/integrity semantics**:
  (A) same ``decision_id`` with an identical canonical record is an idempotent
  no-op; (B) the same ``decision_id`` with different canonical content fails
  closed with ``ProvenanceWriteError``; (C) existing records are never edited
  or replaced; (D) no truncate/rewrite ever occurs.
- **Frozen input**: ``decision_provenance.frozen_input`` builds the canonical
  decision-input snapshot (candidate/market/event identity, outcome,
  end-date, category, research estimate and disposition, entry policy,
  open-position and packet state). Its canonical JSON SHA-256 is stable under
  key reordering and changes on any decision-relevant field change.
- **Fail-closed provenance**: for an actionable bet, the ``attempt`` record is
  written before ``record_candidate_paper_placement``. If provenance
  persistence fails, the placement is refused with the bounded code
  ``provenance-write-failed`` and no ledger mutation occurs.
- **Trade / no-trade / rejected** are explicit bounded actions; no-trade and
  rejection are complete records, never missing data. Existing
  ``skip_reason`` and ``rejection_code`` values are retained verbatim in
  ``reason_code`` / ``placement_rejection_code``; nothing is renamed.
- **Truthful provenance**: ``research_model_id`` stays ``null`` unless
  supplied; profile comes only from staged run metadata; ``event_id`` stays
  ``null`` when genuinely unresolved; ``code_revision`` is ``null`` because
  the operator runtime is not a VCS checkout. No identity is ever guessed.
- **Replay/shadow** (``manus.shadow_replay``) is read-only: the pure
  ``evaluate_frozen_decision`` evaluator mirrors the existing guarded order
  (insufficient-cash -> risk-cap-event -> max-open-positions ->
  duplicate-market-outcome -> packet-position-cap -> category-position-cap ->
  entry-price-out-of-bounds -> spread-too-wide -> edge-below-threshold ->
  non-bet disposition) and produces identical decisions from identical
  inputs. Replay/shadow never append provenance, never touch journals,
  operational state, the scanner network APIs, research transport,
  ``paper_apply``, or the broker adapter, and never fetch live data.
  ``replayability_status`` reports the bounded vocabulary
  ``replayable`` / ``replay-input-incomplete`` / ``provenance-unavailable``.
- **Shadow engines** are typed callables on the frozen input
  (``shadow_replay.ShadowEngine``). The repository bundles no engine, so no
  JEV/AI implementation, network call, or key handling can occur; future
  engines plug in behind explicit operator authorization.
- **Zero import side effects**: importing the new modules performs no I/O,
  network, thread, credential, or directory creation.

### Patch 5F-2: exact source-market → IBKR instrument mapping

5F-2 adds `ibkr/instrument_mapping.py`: a pure, offline, operator-controlled bridge between exact source prediction-market identities and explicitly approved IBKR instruments. Resolution is exact-key lookup only — provider + market_id + event_id + outcome — against a versioned registry (`config/ibkr_instrument_mappings.json`, schema `ibkr-instrument-mappings/v1`). There is no question-text, slug, category, ticker, similarity, LLM, or search-based mapping of any kind, and every failure is bounded: `instrument-mapped`, `instrument-unmapped`, `source-binding-mismatch`, `unsupported-security-type`, `broker-contract-not-found`, `broker-contract-ambiguous`, `broker-contract-mismatch`, `mapping-invalid`, `mapping-ambiguous`.

The registry is deterministic JSON-serializable data. Each entry carries an operator-authored `mapping_id`, a hash-bound `status` (only `active` resolves), the exact source binding with a `source_binding_sha256` semantic fingerprint (provider, market_id, event_id, outcome, expected outcomes, expected end date — never volatile market data), the IBKR target identity (`conid` primary; `sec_type`, `symbol`, `currency` mandatory; `exchange`, `primary_exchange`, `local_symbol`, `trading_class` retained and cross-checked when configured), and an explicit `exposure` of `direction` (long/short) plus `relationship` (DIRECT_UNDERLYING, POSITIVE_PROXY, INVERSE_PROXY, HEDGE, OTHER_EXPLICIT_PROXY). Direction and relationship come only from the approved entry: Yes never implies long, No never implies short, and inverse proxies are never inferred. `entry_sha256` = SHA-256 of the canonical entry; reordering keys does not change it, changing conid/direction/relationship/source does.

Duplicate active exact source routes are rejected (`mapping-ambiguous`); duplicate mapping_id fails closed; unknown fields fail closed everywhere. The first asset-class scope is STK only; other secTypes fail closed as intentional risk reduction before 5F-3. The committed production registry is EMPTY — an empty registry is a valid state, and any real source resolves `instrument-unmapped` rather than a guessed mapping.

Broker verification (`verify_ibkr_contract(mapping, adapter)`) reuses the unchanged 5F-1 read-only `lookup_contract` surface: conId is the primary identity, exactly one match is required, and every configured identity field is cross-checked against the returned normalized contract. No transport is created, EClient is never exposed, no ibapi object leaves the 5F-1 boundary, and no account identifier enters any result. Real Windows/TWS verification is the operator's separate read-only acceptance step; the verifier is unit-tested with fakes only.

The shipped CLI (`python -m ibkr.instrument_mapping inspect --source-file F --registry F` via `--source-file`/`--registry`) is pure and offline; broker verification intentionally remains a Python API to avoid widening the interactive surface. The CLI rejects operational terms (place, order, quantity, size, execute, live, arm, cancel, submit, transmit, and related) while `-h`/`--help` remain available. No existing PAPER flow imports this module: `manus.research_transport`, `manus.paper_runner`, `manus.paper_apply`, `manus.scheduled_paper`, and `core/*` are unchanged, journals and provenance are untouched, and 5F-2 defines typed mapping facts (`mapping_id`, `mapping_sha256`, `source_binding_sha256`, conid/sec_type/symbol/currency, direction, relationship) that 5F-3 can later bind into execution provenance without changing 5E-6 records.

## 5F-2a — conId-primary broker verification + closed CLI allowlist

**Scope.** 5F-2a hardens the 5F-2 seam in two operator-review directions and touches
nothing else: (1) broker verification becomes genuinely conId-primary through a
narrow read-only 5F-1 extension, (2) the mapping CLI's substring safety filter is
replaced by a closed option allowlist with bounded error hygiene.

**conId-primary verification.**
- `ibkr/transport_tws.py` gains ONE private method, `contract_details_by_conid(conid)`,
  mirroring the existing `contract_details()` lifecycle exactly: account verification,
  scoped request-id operation, bounded `_complete_or_fail` timeouts, per-stream
  completion sync, normalized rows, no raw ibapi objects escaping, no Order surface,
  no refactoring of unrelated transport code. The private request uses an ibapi
  Contract with only `conId` (plus exchange) set.
- `ibkr/adapter.py` gains ONE method, `lookup_contract_by_conid(conid)`, preserving
  `lookup_contract(...)` unchanged. It validates the conid (bool/non-int/<=0 →
  `invalid-broker-response` BEFORE any broker I/O), goes through the same
  `_verified_session()` account allowlist boundary as every other 5F-1 read, and maps
  cardinality: 0 → `contract-not-found`, 1 → normalized `_CONTRACT_FIELDS` projection,
  >1 → `contract-ambiguous`. The conId contract identity is returned first and never
  derived from a symbol lookup.
- `verify_ibkr_contract(mapping, adapter)` calls ONLY
  `adapter.lookup_contract_by_conid(mapping["target"]["conid"])`. There is NO symbol
  fallback in either direction: a fake adapter whose symbol lookup raises and whose
  conId lookup succeeds verifies successfully, while an adapter whose conId lookup
  fails fails closed even when a symbol lookup would have succeeded. After the conId
  read, `conid`, `sec_type`, `symbol`, and `currency` plus configured optionals
  (`exchange`, `primary_exchange`, `local_symbol`, `trading_class`) are cross-checked
  as metadata; any mismatch → `broker-contract-mismatch` and the verifier fails closed.
- Broker verification remains a Python API only. The CLI has no verification command.
  Real TWS/Gateway verification is operator-only, on the operator's machine, after
  operator review of this PR.

**Closed CLI allowlist.**
- The mapping CLI accepts exactly `-h`, `--help`, `--source-file FILE`, and
  `--registry FILE`. The parser is built with `allow_abbrev=False`, so option
  abbreviation (`--source-f`, `--reg`, ...) is rejected.
- The previous substring safety filter (which scanned argument VALUES, rejecting
  legitimate path names containing "trade"/"real"/"position"/"size") is REMOVED.
  File-path VALUES are opaque and never scanned. All previously forbidden options
  (`--place-order`, `--quantity`, `--size`, `--execute`, `--live`, `--arm`,
  `--cancel`, `--submit`, `--transmit`, `--place`, `--buy`, `--sell`, `--trade`,
  `--position`, `--real`) are rejected by the parser itself — they are simply outside
  the closed allowlist.
- Error hygiene: parser and mapping rejections exit nonzero with a bounded operator
  message; `MappingError` and `AdapterError` are caught in `main()`; there is no
  Python traceback for a normal CLI rejection, and no local source paths, credentials,
  or account identifiers appear in any error output. `--help` exits 0; a successful
  inspection exits 0 with bounded JSON.

**Invariants preserved.** PAPER only; no order placement or cancel ever (static audit
re-run clean); the production registry `config/ibkr_instrument_mappings.json` remains
EMPTY; `journal/forecasts.jsonl` and `journal/ledger.jsonl` remain byte-identical;
Windows compatibility (pathlib, UTF-8, no Unix-only semantics); all 5F-1/5F-2 test
coverage preserved with the IBKR suite extended, not weakened.

## Patch 5F-3a — explicitly armed IBKR PAPER execution boundary (operator-approved)

The PAPER-only IBKR submission boundary is an ADDITIONAL operational truth source for Phil decisions, with these boundary properties:

- `placeOrder` exists ONLY inside `ibkr/paper_transport.py` (private `TwsPaperExecutionTransport`), the only module constructing an `ibapi.order.Order`. `ReadonlyIbkrAdapter` and all other transports remain incapable of broker mutation; no cancel/modify/exercise/transfer/LIVE path exists anywhere.
- The executor (`ibkr/paper_execution.py`) is NOT importable from the automated pipeline: `manus/paper_runner.py`, `manus/paper_apply.py`, and `scheduled_paper_task.py` must never import `ibkr.paper_execution` or `ibkr.paper_transport` (tested). This is a manually invoked boundary only.
- Submission requires, in strict order, ALL of: current-invocation `--arm-paper`; exact `--confirm-execution-id`; valid closed-schema execution intent (`ibkr-paper-execution-intent/v1`); operator config `environment == "PAPER"` (declared label only; ports/prefixes are never consulted); approved mapping hash equal to the intent's `mapping_sha256`; no prior receipt for that execution id (uncertain ⇒ fail closed, never retried; submitted ⇒ duplicate rejected); fresh conId re-verification through the 5F-2 read-only verifier; no broker evidence already carrying the deterministic `order_ref` (`phil5f3-<32hex>`).
- Order identity is broker-authoritative: the `nextValidId` handshake seed (retained only inside the paper wrapper; the read-only wrapper still discards it) is used as the order id and never invented by Phil.
- Hard construction envelope: STK/USD/SMART, long→BUY only (short ⇒ `paper-short-not-supported`; never inferred from Yes/No outcomes), LMT, DAY, whole shares, `outsideRth=False`, `transmit=True` fixed, absolute Decimal USD 100.00 notional cap, no float monetary input.
- Exactly one durable `submission-attempted` receipt is appended BEFORE `placeOrder`; outcomes are `acknowledged`/`rejected`/`uncertain` with a bounded wait and per-order correlation; NO retry of any kind exists.
- Receipts: append-only, idempotent-per-`event_id`, `record_sha256`-verified JSONL under the fixed external root `%LOCALAPPDATA%\phil-manus\ibkr-paper-execution`, serialized by a new fixed writer lock (`paper-execution-writer`); they preserve the decision→mapping→intent→orderRef chain and never contain raw account ids, secrets, config paths, or broker error text. Existing protected journals are NOT modified by this boundary.
- Read-only additions for reconciliation: normalized `open_orders()`/`executions()` rows gained an `order_ref` field (additive only; semantics of existing fields unchanged). The read-only transport interface carries NO mutation-named member (5F-3a1 removed the 5F-3a `submit_order` stub): `ReadonlyTransport`, `TwsTransport`, and `ReadonlyIbkrAdapter` expose no `submit_order`/`place_paper_order` (structural absence, tested), and `place_paper_order` exists ONLY on `TwsPaperExecutionTransport`.
- Write-session account allowlist (5F-3a1): the session that owns `placeOrder` independently verifies `managed_accounts == [expected_account_id]` — exactly one account, exactly the configured id, never inferred from a DU prefix, port, environment, or account type — BEFORE order-id allocation or `placeOrder`; zero/multiple/unexpected accounts fail closed with ZERO placements, and a prior successful read-only verification never authorizes a later mismatched write session.
- CLI is closed-option (`allow_abbrev=False`): `submit` with `--intent-file`, `--mapping-file`, `--confirm-execution-id`, `--arm-paper`; no live/real/force/override/retry/transmit/market/sell/short flags; failures are bounded (no traceback, no path/secret leakage).
- Smoke exercises (5F-3a1) must NOT modify the production mapping registry: `config/ibkr_instrument_mappings.json` stays exactly `{"schema_version": "ibkr-instrument-mappings/v1", "mappings": []}`. Independent acceptance uses an external temporary operator-controlled mapping (`%TEMP%\phil-5f3a-smoke\mapping.json`) and intent (`%TEMP%\phil-5f3a-smoke\intent.json`) via the CLI's `--mapping-file`/`--intent-file`; no production mapping may be committed for a smoke.

## Patch 5F-3a2 — atomic execution claim, session cleanup, Windows portability

- The authoritative placement idempotency boundary is the atomic execution claim: `ibkr/paper_execution_state.claim_submission_attempt` performs, under ONE fixed cross-process OS lock (bounded wait, lock covering ONLY the durable claim and no broker I/O), the authoritative state re-check plus exactly one durable fsynced `submission-attempted` append. Only a `claimed` result permits continuation toward the write session and `placeOrder`; every other bounded result (`execution-already-claimed`, `execution-uncertain`, `receipt-write-failed`) stops the process BEFORE placement. The earlier non-atomic state read remains an optimization only. Ownership is by `execution_id` alone — never by `event_id` idempotency, timestamps, or clock precision.
- Broker-rejected executions remain one-shot: a repeated invocation with the same execution id fails closed (`execution-already-submitted`) with zero place calls; a later attempt requires a new execution intent.
- The write-capable TWS session is disconnected exactly once through an outer `finally` on every outcome path (acknowledged, done, rejected, account mismatch, timeout, uncertain, unexpected exception, partial connection); a best-effort cleanup failure never masks the original result or error code.
- The executor-level cross-process regression runs two real OS worker processes (spawn-compatible) racing one execution id; exactly one place call happens across both processes, the loser stops before placement, and exactly one `submission-attempted` receipt exists. Windows path portability of the architectural AST tests is achieved with `as_posix()` normalization (no weakening, no skips, no substring matching).
