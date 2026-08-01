# test

status: current
source: test/ — 26 test functions across sources.js/destinations.js/interactions.js, all passing

## Summary

`test/` is a separate Apps Script project whose only job is to exercise
`tinyDeskData.move()` against every source and destination behavior
documented in [move.md](move.md), and assert on the result — a real,
re-runnable regression check for `move()`, as opposed to the hand-made
illustration predating spec-driven development (see
[README.md#provenance](README.md#provenance)), which isn't exercised
repeatably by anything.

`model` and `orchestrate` are out of scope for `test/` for now — `move` is
first, matching the order specs are being written in (see
[README.md#index](README.md#index)).

## Behavior

### Why not a full source × destination cross product

`move.md` documents `_moveGetData` (source → `data`) and `_moveLoadData`
(`data` → destination) as two independent stages that only interact through
the shape of `data` (an array of rows, first row headers, all values
strings). Source behavior and destination behavior don't interact with each
other — a bug in the Excel-to-Sheet conversion path doesn't depend on which
destination the data eventually lands in. A literal cross product (9
documented source variants × 9 documented destination variants) would
mostly re-test the same stage twice per case for no added coverage.

Instead, `test/` uses:

1. **One test function per documented source variant**, all writing to the
   same simple, easy-to-assert-on destination (`drive`/`csv` — no
   flush/typing/partitioning quirks of its own to confound the assertion).
2. **One test function per documented destination variant**, all reading
   from the same simple, deterministic source (a `here`/`.gs` literal array
   — no external file/service dependency to fetch).
3. **A small number of explicit interaction tests** for behavior that
   `move.md`'s "Edge cases & known limitations" section calls out as
   asymmetric or cross-cutting (write-disposition default differs by
   destination type, `errorResult` not checked, partitioning, temp-file
   cleanup) — these need a real end-to-end run because the thing under test
   is the combination, not either stage alone.

This gets full coverage of every documented behavior in `move.md` at
roughly `N + M` test functions instead of `N × M`, while still having a few
true end-to-end combinations for the cases where the combination itself is
the risk.

### Source variants (→ `drive`/`csv` destination)

| # | `source.where` | Variant | Expected outcome |
|---|---|---|---|
| S1 | `drive` | Google Sheet, default sheet | data matches fixture sheet contents — note the fixture's `"10.50"`/`"20.00"` amount cells round-trip as `"10.5"`/`"20"`, per `move.md`'s Sheets auto-type-coercion edge case |
| S2 | `drive` | Google Sheet, named `sheet_name` | data matches the named (non-first) sheet |
| S3 | `drive` | Excel `.xlsx` | data matches fixture; temp converted Sheet is cleaned up (`Drive.Files.remove` called — verify temp file does not persist in the test folder after the run) |
| S4 | `drive` | CSV | data matches fixture, including a field with a comma/quote/newline to exercise quoting on the way out |
| S5 | `drive` | unrecognized mime type (e.g. a plain `.txt` file) | `_moveGetData` throws a descriptive `Error` naming the `file_id` and unsupported mimeType (documented in `move.md`) — test asserts the throw happens and the message names the mimeType |
| S6 | `here` | `.sql` run against BigQuery | data matches a small, known query result against the `tinydeskdata-test` dataset |
| S7 | `here` | `.gs` literal array | data matches the literal exactly |
| S8 | `here` | unrecognized extension | `_moveGetData` throws a descriptive `Error` naming the file and extension, same as S5 |
| S9 | `bigquery` | read existing BigQuery table | data matches a small seeded table (order-independent — move()'s query is a bare `select *` with no `ORDER BY`, so BigQuery doesn't guarantee row order), including header row from schema field names |
| S10 | (any) | unrecognized `source.where` | `_moveGetData` throws a descriptive `Error` naming the unrecognized value, same as S5 |

### Destination variants (`here`/`.gs` literal → destination)

| # | `destination.where` | Variant | Expected outcome |
|---|---|---|---|
| D1 | `drive`/`sheets` | existing file, `mode: 'overwrite'` | sheet content is cleared and overwritten from row 1 |
| D2 | `drive`/`sheets` | existing file, `mode: 'append'` | new rows start at `getLastRow() + 1`, prior rows untouched |
| D3 | `drive`/`sheets` | `mode` omitted (default `'create'`), no `sheet_name` | new spreadsheet created, named `file_name`, default sheet keeps its name |
| D4 | `drive`/`sheets` | `mode` omitted (default `'create'`), with `sheet_name` and `folder_id` | new spreadsheet created, default sheet renamed, file moved into `folder_id` |
| D5 | `drive`/`csv` | plain write | CSV file created in `folder_id`, UTF-8 BOM present, fields with `,`/`"`/newline correctly quoted |
| D6 | `drive`/`csv` | `file_name` without `.csv` | `.csv` is appended to the created file's name |
| D7 | `drive` | unrecognized `file_type` | `_moveLoadData` throws a descriptive `Error` naming the unrecognized `file_type` (documented in `move.md`): assert the throw happens, the message names the `file_type`, and nothing is written |
| D8 | `bigquery` | `write_disposition` omitted | defaults to append (`WRITE_APPEND`) — row count grows, prior rows untouched |
| D9 | `bigquery` | `write_disposition: 'truncate'` | prior rows gone, only new rows present |
| D10 | `bigquery` | `partition_column` set | destination table has day partitioning on that column, and its schema type is `DATE` while all other columns are `STRING` |
| D11 | (any) | unrecognized `destination.where` | `_moveLoadData` throws a descriptive `Error` naming the unrecognized value, same as D7 |
| D12 | `drive`/`sheets` | unrecognized `mode` | `_moveLoadData` throws a descriptive `Error` naming the unrecognized `mode` value (documented in `move.md`), same shape as D7/D11 — assert the throw happens, the message names the `mode` value, and nothing is written |

### Interaction tests (explicit end-to-end combinations)

| # | Combination | What it proves |
|---|---|---|
| I1 | `bigquery` (existing table) → `drive`/`csv` | full round trip through both stages together with real BigQuery-shaped string data, including the destination's UTF-8 BOM byte-for-byte (note: `Blob.getBytes()` in Apps Script returns *signed* bytes, so the BOM check must normalize with `& 0xFF` before comparing to `0xEF`/`0xBB`/`0xBF`) |
| I2 | `here`/`.gs` literal → `bigquery`, `write_disposition` omitted vs. same literal → `drive`/sheets, `mode: 'overwrite'` | same source, both destinations, run back to back — makes the documented default-behavior asymmetry (Sheets requires an explicit `mode`; BigQuery defaults to append) an assertion instead of a note in a doc |
| I3 | `here`/`.gs` literal with an unparseable value (`"not-a-date"`) for a `partition_column` → `bigquery` | confirms `_moveLoadData` now checks `status.errorResult` once the load job reports "done" and throws a descriptive `Error` (including BigQuery's own message) instead of treating a failed load as success. **Not** a column-count mismatch or a `NOT NULL` violation — see I4 and the notes below. |
| I4 | `here`/`.sql` seeded to produce a column-count mismatch against an existing destination table → `bigquery` | confirms this specific kind of failure is instead rejected **synchronously** by `BigQuery.Jobs.insert` and surfaces as a real thrown exception — discovered while building I3 (the original design), and folded back into `move.md`'s edge cases since it wasn't documented before |

## Config / Interface

`test/` has its own `appsscript.json` (advanced services `Drive` v3,
`BigQuery` v2, and the corresponding OAuth scopes) and `.clasp.json`
pointing at a dedicated Apps Script project, so pushing/running tests can't
disturb anything else.

Backing resources are dedicated, separate from any other project:

- GCP project: `tinydeskdata-test` (BigQuery API + Drive API enabled,
  billing linked so query/load jobs run).
- BigQuery dataset: `tinydeskdata_test` in that project, holding seeded
  fixture tables and the tables each destination test writes into.
- Drive: a dedicated test folder holding fixture files (a Sheet with a
  named second sheet, an `.xlsx`, a `.csv` with quoting edge cases, and an
  unsupported `.txt`) plus a subfolder used as the write target for `drive`
  destination tests, kept separate from any other project's resources.

Each test function follows the same shape: build the `move()` payload for
one row of the tables above, call `tinyDeskData.move(payload)`, then read
back the actual destination state (via `SpreadsheetApp`, `DriveApp`, or a
BigQuery query) and compare against the expected outcome, logging
pass/fail. There's no assertion framework dependency available in Apps
Script by default, so this stays a plain function that throws (or logs) on
mismatch — consistent with there being no test runner/CI in this repo
(see `CLAUDE.md`).

## Edge cases & known limitations

- BigQuery jobs (`bigquery` source/destination, `here`/`.sql` source)
  are not free or instant — the test suite has real latency and (small)
  cost per run, unlike a typical unit test suite. Keep fixture tables tiny.
- `_moveGetData` now throws a descriptive `Error` on unrecognized sources
  (S5, S8, S10) naming the unrecognized value, instead of leaving `data`
  `undefined` for `_moveLoadData` to fail on later with a raw,
  undescriptive `TypeError`. Those tests assert both that the throw happens
  and that the message names the offending value, so a future regression
  back to a generic/undescriptive error would be caught.
- All `sql_platform`/`platform: 'bigquery'` payloads across S9, D8-D10, and
  I1-I4 were rewritten to `where: 'bigquery'`, and D1/D2/I2's
  `new_file_flag`/`write_disposition` Sheets payloads to `mode`, per
  `move.md`'s config-shape change. D12 is new: it covers the `mode`
  validation that shape change introduced (an unrecognized `mode` now
  throws, same as D7's unrecognized `file_type`), which didn't exist as
  documented behavior before.
- I3 used to deliberately exercise a known bug-shaped behavior (BigQuery
  load failure not surfaced) as a tripwire against an accidental fix — see
  git history for that version of this spec. It now asserts the fixed
  behavior instead: `_moveLoadData` checks `status.errorResult` and throws.
- I3 went through two wrong designs before landing on the current one, both
  worth keeping as history since they mark real discoveries about
  `move()`, not just test bugs:
  1. First attempt: a column-count schema mismatch (matching `move.md`'s
     original wording, "e.g. a column count mismatch"). Running it against
     real BigQuery showed `BigQuery.Jobs.insert` rejects that case
     **synchronously**, so `move()` throws — the opposite of "job reports
     done, error unsurfaced." This became I4 instead.
  2. Second attempt: a `NOT NULL` constraint violation, assuming that (unlike
     column count) would only be checked once the job actually ran. It
     didn't — BigQuery also rejects a schema/mode mismatch (the load's
     field is implicitly `NULLABLE`, the destination column is `REQUIRED`)
     synchronously at submission, same as I4.
  3. What actually reaches the async "job completes, error unsurfaced" path:
     a value that's structurally valid for its declared type but not
     semantically parseable — an unparseable string in a `DATE`-typed
     `partition_column`. The schema itself is valid (a `DATE` field is a
     normal thing to declare), so `Jobs.insert` accepts the job; only
     actually converting `"not-a-date"` to a date, per row, can fail — and
     that only happens once the job runs. This is now I3.

  All three failure modes are real and now documented in `move.md`'s edge
  cases: two throw synchronously (I4-shaped), one doesn't (I3-shaped).
- Excel conversion (S3) leaves a real temp file in Drive if the script
  dies between create and remove (documented in `move.md`); the test only
  checks the happy path (temp file gone after a normal run), not the crash
  case, since that would require killing the script mid-execution.

## Open questions

- Whether `model`/`orchestrate` get their own analogous `test/` coverage
  later, or whether `test/` grows to cover them once `move`'s coverage is
  considered done — deferred until `move` coverage lands, per
  [README.md#index](README.md#index).
