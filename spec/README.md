# Specs

## Context

tinydeskdata is a small Google Apps Script library for moving tabular data
between Drive (Sheets, CSV, Excel), local script files (`.sql`, `.gs`), and
BigQuery — a lightweight, dbt-inspired ELT layer (see `example/` for staging
models like `stg_customers.sql.html`) that runs entirely inside Apps Script.

It exists because there was no budget or infra for a real ELT tool
(dbt/Fivetran-style), and the data already lives in Sheets/Drive as part of
existing Apps Script workflows — pulling it into a separate hosted service
wasn't practical. It's also a hands-on way to learn data engineering
concepts (staging models, write dispositions, partitioning) by building them
from scratch. This context should inform scope decisions: prefer solutions
that work within Apps Script's constraints over ones that assume a
conventional server/database environment.

## Philosophy

tinydeskdata comes out of working as a data analyst / analytics engineer in
environments where data **volume** is small, but the other V's usually
associated with "big data" — **variety**, **velocity**, **veracity** — are
still present: several source formats and systems, data that changes on its
own cadence, and quality that can't be assumed. Classic big-data tooling is
built to scale volume; it's overkill for these environments and brings
problems they don't have the volume to justify.

At the same time, these environments are rarely staffed with people who can
own a full CI/CD pipeline, DevOps tooling, or dedicated data infrastructure —
often it's one person doing analytics work alongside everything else, inside
tools like Google Sheets and Apps Script that are already there.

tinydeskdata is the answer to that gap: a library to **Move** (extract and
load), **Transform** (model), and **Orchestrate** pipeline steps, declaratively,
entirely inside Google Apps Script — no external infra, no deploy pipeline,
no DevOps knowledge required to run or maintain it. `move` is the first
pillar implemented; `model` (transform) and `orchestrate` are the other two
named in the [Index](#index) below.

This folder holds specs for tinydeskdata, following a spec-driven development workflow:

1. Before changing or adding behavior, write or update the relevant spec in this folder.
2. Get the spec reviewed/agreed (even informally) before implementation starts.
3. Implement against the spec; update the spec in the same PR if reality diverges during implementation.

## Conventions

- One file per functional area (e.g. `move.md`, `model.md`, `orchestrate.md`).
- Each spec follows `_template.md`.
- `status: draft` = still being defined, not authoritative.
  `status: current` = matches shipped behavior, safe to treat as source of truth.
  `status: proposed` = describes behavior that does not exist yet.
- Specs describing *existing* behavior should cite the source file/function they document, so drift can be caught by re-reading the code.

## Index

| Spec | Status | Covers |
|---|---|---|
| [move.md](move.md) | current | `tinyDeskData.move` — data ingestion/export between Drive, local files, and BigQuery |

`model` and `orchestrate` are intentionally out of scope for now — work is
focused on `move` first.
