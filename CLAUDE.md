# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

tinydeskdata is a single-file Google Apps Script library (`tinydeskdata.js`)
providing Move / Model / Orchestrate primitives for small-data pipelines that
run entirely inside Apps Script, with no external infra or CI/CD. See
`spec/README.md` for the full context and design philosophy behind why it
exists, and `spec/` in general for the spec-driven development workflow this
project follows — read/update the relevant spec **before** changing or
adding behavior.

## Architecture

Everything lives in one IIFE in `tinydeskdata.js`, which exposes a global
`tinyDeskData` with three public methods; everything else is a private
helper (`_moveGetData`, `_modelCompile`, etc.):

- **`move(obj)`** — extract-and-load. Two-step pipeline: `_moveGetData`
  reads a 2D array from `obj.source`, `_moveLoadData` writes it to
  `obj.destination`. Sources/destinations: `drive` (Sheets/Excel/CSV),
  `here` (local `.sql` run against BigQuery, or `.gs` eval'd as a JS
  expression), `sql_platform` (a BigQuery table). Fully documented in
  `spec/move.md` — treat that file as the authoritative contract, including
  its documented edge cases (silent no-ops on unrecognized config, no
  `errorResult` check on BigQuery loads, etc.).
- **`model(obj)`** — a small dbt-like SQL modeling engine. Compiles
  `.sql.html` files (see `example/*.sql.html` — a hand-made illustration of
  the shape, not a fixture exercised by any test) using regex-based templating
  for `{{ ref('x') }}`, `{% set x = [...] %}`, `{% if is_incremental() %}`,
  and `{% for %}` (`_modelCompile`); resolves model dependencies from
  `ref()` calls (`_modelSetDependencies`) and topologically sorts them
  (`_topologicalSort`); executes each model into a `<name>__tmp` table, runs
  column tests (`unique`, `not null`, `accepted_values`, `relationships`)
  before promoting, and materializes as `table` / `view` / `insert` /
  `incremental` (with `append`, `merge`, or `delete+insert` strategies).
- **`orchestrate(obj)`** — runs named `nodes` (each wrapping a `move` or
  `model` payload) in dependency order via the same topological sort,
  timestamps each node's start/end, and writes the run as a JSON log to a
  Drive folder — see `logs/*.json` for the shape (one hand-made sample run,
  kept for shape reference only — not regenerated or checked by anything).

None of these are unit-testable in isolation: they call Apps Script global
services directly (`SpreadsheetApp`, `DriveApp`, `Drive`, `BigQuery`,
`HtmlService`, `Utilities`, `Session`), so the code only runs inside the
Apps Script runtime.

## How it's consumed

The library isn't installed as an npm/clasp package — it's pulled into a
consumer's Apps Script project at runtime via
`eval(UrlFetchApp.fetch('https://raw.githubusercontent.com/.../tinydeskdata.js').getContentText())`
(see `example/_.js`).

`example/` is itself a real, separate Apps Script project (own
`appsscript.json` + `.clasp.json`, deployed via `clasp`) that exercises all
three pillars end-to-end against a "jaffle shop" dataset. **It predates
spec-driven development and is a hand-made illustration, not a test
suite** — see `spec/README.md`'s Provenance section. It's kept around as a
worked example of what a consumer project looks like, and `logs/*.json` is
one hand-made sample run kept only as a shape reference. Neither is
exercised repeatably or checked by anything — there is no CI/CD in this
repo.

`test/` (see `spec/test.md`) is the actual, deliberately-maintained test
bed: a separate Apps Script project whose only job is to exercise `move()`
against every documented source/destination combination from `spec/move.md`
and assert on the result. Unlike `example/`, it's meant to be re-run after
changes to `_moveGetData`/`_moveLoadData`, not just read as a reference.

## Making and verifying a change

1. Check `spec/` for the relevant spec first; update it alongside the code
   if behavior changes or diverges from what's documented.
2. Edit `tinydeskdata.js` directly.
3. If the change touches `move()`: push `test/` with `clasp push`, run the
   relevant combination function(s) (see `spec/test.md` for the matrix and
   how to run a subset) from the Apps Script editor or via `clasp run`, and
   confirm against the dedicated `tinydeskdata-test` BigQuery project and
   the test Drive fixtures — this is the repeatable regression check for
   `move()`.
4. To sanity-check the full pipeline end-to-end (move + model +
   orchestrate together): push `example/` with `clasp push`, run `teste()`
   (in `example/_.js`) from the Apps Script editor or via `clasp run`, then
   confirm it succeeded by checking BigQuery table/row state and reading
   the resulting log JSON from the configured Drive
   `log_destination.folder_id` (compare against `logs/*.json` for the
   expected shape). This is a manual smoke test against real "jaffle shop"
   data, not a repeatable regression suite — treat a pass/fail here as
   informative, not authoritative.
5. A Claude Code session can do the BigQuery/Drive inspection for either of
   the above itself — `bq ls` / `bq query` if `gcloud`/`bq` are installed
   and authenticated (`gcloud auth login`) against the relevant project,
   and reading log/fixture JSON directly if the Google Drive connector is
   connected (`/mcp`) — instead of relying on you to report back what you
   see. This tooling is local, per-session setup, not part of the repo or
   any CI/CD.

## Git workflow

Never commit directly to `master`. Always create a feature branch and open
a PR — even for small or doc-only changes like edits to this file.

Branch names and PR titles use a `type: description` (PR title) /
`type/description` (branch) nomenclature, where `type` is one of:

- `feature` — new behavior
- `fix` — bug fixes, including turning an undocumented/silent failure into
  correct or descriptive behavior
- `refactor` — internal restructuring with no behavior change
- `docs` — documentation-only changes (including spec/CLAUDE.md edits)
- `test` — changes to `test/` (or, hypothetically, other test tooling)
- `chore` — maintenance that doesn't fit the above (dependency bumps,
  formatting, etc.)

E.g. branch `fix/move-error-messages`, PR title
`fix: replace silent failures in move() with descriptive errors`.
