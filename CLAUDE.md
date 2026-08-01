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
  expression), `bigquery` (a BigQuery table). Fully documented in
  `spec/move.md` — treat that file as the authoritative contract, including
  its documented edge cases.
- **`model(obj)`** — a small dbt-like SQL modeling engine. Compiles
  `.sql.html` files using regex-based templating for `{{ ref('x') }}`,
  `{% set x = [...] %}`, `{% if is_incremental() %}`, and `{% for %}`
  (`_modelCompile`); resolves model dependencies from `ref()` calls
  (`_modelSetDependencies`) and topologically sorts them
  (`_topologicalSort`); executes each model into a `<name>__tmp` table, runs
  column tests (`unique`, `not null`, `accepted_values`, `relationships`)
  before promoting, and materializes as `table` / `view` / `insert` /
  `incremental` (with `append`, `merge`, or `delete+insert` strategies).
- **`orchestrate(obj)`** — runs named `nodes` (each wrapping a `move` or
  `model` payload) in dependency order via the same topological sort,
  timestamps each node's start/end, and writes the run as a JSON log to a
  Drive folder.

None of these are unit-testable in isolation: they call Apps Script global
services directly (`SpreadsheetApp`, `DriveApp`, `Drive`, `BigQuery`,
`HtmlService`, `Utilities`, `Session`), so the code only runs inside the
Apps Script runtime.

## How it's consumed

The library isn't installed as an npm/clasp package — it's pulled into a
consumer's Apps Script project at runtime via
`eval(UrlFetchApp.fetch('https://raw.githubusercontent.com/.../tinydeskdata.js').getContentText())`.

`test/` (see `spec/test.md`) is the actual, deliberately-maintained test
bed: a separate Apps Script project whose only job is to exercise `move()`
against every documented source/destination combination from `spec/move.md`
and assert on the result. It's meant to be re-run after changes to
`_moveGetData`/`_moveLoadData`, not just read as a reference — there is no
CI/CD in this repo.

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
   - `test/_.js`'s `importLib_()` always fetches `tinydeskdata.js` from
     GitHub raw **`master`**, never local edits or the current feature
     branch (mirrors how real consumers use the library — see "How it's
     consumed"). So running `test/` as-is before a change is merged only
     re-tests the old, already-shipped code. To verify pre-merge: push the
     feature branch to `origin` (raw.githubusercontent.com serves any
     pushed branch), temporarily point `importLib_()`'s URL at that branch,
     `clasp push` `test/`, run the tests, then revert the URL back to
     `master` before merging — it's a test-harness-only edit and shouldn't
     be committed/shipped.
4. A Claude Code session can do the BigQuery/Drive inspection for the
   above itself — `bq ls` / `bq query` if `gcloud`/`bq` are installed and
   authenticated (`gcloud auth login`) against the relevant project, and
   reading fixture state directly if the Google Drive connector is
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
