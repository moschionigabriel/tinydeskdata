# move

status: current
source: tinydeskdata.js (`_moveGetData`, `_moveLoadData`, exposed as `api.move`)

## Summary

`move(obj)` copies tabular data from one source to one destination. It reads
a 2D array of rows from `obj.source`, then writes that same array to
`obj.destination`. No transformation happens in transit beyond format
conversion (e.g. sheet → array, array → CSV).

## Behavior

`move` is a two-step pipeline: `_moveGetData(obj)` returns `data` (an array
of rows, each row an array of cell values, first row is headers), then
`_moveLoadData(obj, data)` writes it to the destination. Source reads use
`getDisplayValues()` or platform equivalents, so everything is effectively
read as text/display strings, not typed values.

### Sources (`obj.source.where`)

- **`drive`** — reads a file by `obj.source.config.file_id`:
  - Google Sheet (`application/vnd.google-apps.spreadsheet`): opens
    `obj.source.config.sheet_name` or the first sheet, returns
    `getDataRange().getDisplayValues()`.
  - Excel (`.xlsx`,
    `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`):
    converts to a temporary Google Sheet via `Drive.Files.create`, reads it
    the same way, then deletes the temp file (`Drive.Files.remove`).
  - CSV (`text/csv`): reads the blob as UTF-8 and parses with
    `Utilities.parseCsv`.
  - Any other mime type: throws a descriptive `Error` naming the `file_id`
    and the unsupported `mimeType`.

- **`here`** — reads a local project file:
  - `.sql` file with `obj.source.config.platform == 'bigquery'`: loads the
    file via `HtmlService.createHtmlOutputFromFile`, strips newlines, runs it
    as a BigQuery query (`useLegacySql: false`), polls until `jobComplete`,
    returns rows with a header row prepended from the query's schema field
    names.
  - `.gs` file: loads the file's raw content and `eval`s it wrapped in
    parens — the file's content is expected to be a JS expression
    (typically an array literal) that becomes `data` directly.
  - `obj.source.config.parent_folder` is prefixed onto the file name if
    present.
  - Any other extension (including a `.sql` file whose `platform` isn't
    `'bigquery'`): throws a descriptive `Error` naming the file and its
    extension.

- **`bigquery`** — reads an entire existing table: runs
  `select * from {schema_name}.{table_name}` against BigQuery
  (`obj.source.config.credentials.project_id`), polls until complete,
  returns rows with a header row from the schema.

Any `obj.source.where` value not in the above list throws a descriptive
`Error` naming the unrecognized value.

### Destinations (`obj.destination.where`)

- **`drive`**:
  - `file_type == 'sheets'`: `mode` selects one of three explicit behaviors
    (default `'create'` if omitted):
    - `mode == 'overwrite'` or `mode == 'append'`: opens an existing
      spreadsheet by `file_id`, or looks it up by `file_name` via
      `DriveApp.getFilesByName(...).next()` if no `file_id` given. Selects
      `sheet_name` or the first sheet.
      - `mode == 'append'`: writes starting at `sheet.getLastRow() + 1`.
      - `mode == 'overwrite'`: clears all content (`clearContent()`) and
        writes from row 1.
    - `mode == 'create'` (or omitted): creates a brand new Google Sheet
      named `file_name`, optionally renames the default sheet to
      `sheet_name`, writes data from row 1, and moves the file into
      `folder_id` if set.
    - Any other `mode` value: throws a descriptive `Error` naming the
      unsupported value — nothing is written.
    - Calls `SpreadsheetApp.flush()` at the end.
  - `file_type == 'csv'`: builds a CSV string in memory (quoting fields that
    contain `,`, `"`, or newline, doubling internal quotes), prepends a
    UTF-8 BOM, and creates a file named `file_name` (`.csv` appended if
    missing) in `folder_id` (or the Drive root folder if omitted).
  - Any other `file_type`: throws a descriptive `Error` naming the
    unsupported value.

- **`bigquery`**:
  - Sanitizes header names to `[a-zA-Z0-9_]` only (all other chars → `_`) to
    derive BigQuery column names.
  - Builds a schema where every column is typed `STRING`, **except** the
    column matching `obj.destination.config.partition_column`, which is
    typed `DATE`.
  - Converts each data row into a JSON object (newline-delimited JSON load
    format) with all values coerced to `String(...)` (or `null`).
  - Loads via `BigQuery.Jobs.insert` with
    `writeDisposition: 'WRITE_' + (write_disposition || 'append').toUpperCase()`
    — default write disposition for BigQuery destinations is **append**.
  - If `partition_column` is set, adds day-partitioning
    (`timePartitioning: { type: 'DAY', field: partition_column }`).
  - Polls `BigQuery.Jobs.get` until `state === 'DONE'`, then checks
    `status.errorResult` and throws a descriptive `Error` (including
    BigQuery's own error message, and the target `schema_name.table_name`)
    if the load job failed.

Any `obj.destination.where`/`file_type`/`mode` combination not covered
above throws a descriptive `Error` naming the unrecognized value(s) —
nothing is written.

## Config / Interface

```js
{
  source: {
    where: 'drive' | 'here' | 'bigquery',
    config: {
      // drive:
      file_id,               // required
      sheet_name,             // optional, defaults to first sheet
      // here:
      parent_folder,          // optional
      file_name,               // required, extension determines handling (.sql | .gs)
      platform,                 // required for .sql, must be 'bigquery'
      credentials: { project_id },
      // bigquery:
      schema_name, table_name,
      credentials: { project_id },
    }
  },
  destination: {
    where: 'drive' | 'bigquery',
    config: {
      // drive/sheets:
      file_type: 'sheets' | 'csv',
      file_id,                    // sheets, existing file (mode: 'overwrite' | 'append')
      file_name,                  // sheets (mode: 'create', or lookup for 'overwrite'/'append' if no file_id) or csv
      sheet_name,                 // sheets, optional
      mode: 'create' | 'overwrite' | 'append',  // sheets, default 'create'; anything else throws
      folder_id,                  // sheets (mode: 'create') / csv destination folder
      // bigquery:
      credentials: { project_id },
      schema_name, table_name,
      write_disposition,          // bigquery: 'append' (default) | 'truncate' | ...
      partition_column,           // optional, forces DATE type + day partitioning
    }
  }
}
```

## Edge cases & known limitations

- Unrecognized `source.where`/mime type/extension throws a descriptive
  `Error` from `_moveGetData` itself (naming the unrecognized value and, for
  `drive`/`here`, the `file_id`/file name involved) rather than letting
  `_moveLoadData` fail later with a raw `TypeError` on `data[0].length`.
- Unrecognized `destination.where`/`file_type` throws a descriptive `Error`
  from `_moveLoadData` naming the unrecognized value(s) — this used to be a
  **silent no-op** (no error, no data written, easy to misconfigure and not
  notice); it now fails loudly instead.
- All source reads use display values (strings), so numeric/date typing is
  lost on the way in; BigQuery destination columns are always `STRING`
  (except the partition column, forced to `DATE`).
- Writing to a Sheets destination goes through `Range.setValues(data)`,
  which lets Sheets auto-detect cell types on write: numeric-looking
  strings (`"1"`, `"10.50"`) are silently stored as real numbers, not text.
  For whole numbers this is invisible (a number cell showing `"1"` looks
  identical to a text cell showing `"1"`), but values with trailing
  zeros/fixed decimals lose that formatting (`"10.50"` → displays as
  `10.5`) — a lossy round trip if that sheet is later re-read as a `drive`
  source (which itself reads via `getDisplayValues()`, so it sees the
  post-coercion display value, not the original string).
- BigQuery load job failures are checked via `status.errorResult` once the
  job reaches `DONE`, and `_moveLoadData` throws a descriptive `Error`
  (BigQuery's own error message plus the target `schema_name.table_name`)
  if the load failed. This covers failures BigQuery only detects once the
  job actually runs and tries to process row data — e.g. a value that isn't
  parseable as its declared column type, such as a non-date string in a
  `partition_column` (typed `DATE`); those used to complete as `DONE` with
  an unchecked `errorResult`, silently loading zero rows, and now throw
  instead. Separately, failures BigQuery's API rejects at job *submission*
  time, before any row is even looked at — the load schema's column count
  not matching an existing destination table, or a field's implicit
  `NULLABLE` mode conflicting with a `REQUIRED` column on the destination —
  were already surfaced as a normal thrown exception before this change,
  since `BigQuery.Jobs.insert` itself throws for those, before
  `_moveLoadData`'s polling loop is ever reached — verified empirically via
  `test/` (see `spec/test.md`'s I3/I4 notes for how this was narrowed down).
- Default write behavior differs by destination type and isn't symmetric:
  Sheets' `mode` defaults to `'create'` (a brand new file) when omitted —
  targeting an existing file always requires explicitly passing
  `mode: 'overwrite'` or `mode: 'append'`. BigQuery's `write_disposition`
  defaults to `'append'` **onto the existing table** when omitted; there's
  no BigQuery equivalent of Sheets' "create a new one" default.
- Sheets' `mode`, `file_type`, and `destination.where`/`source.where` are
  the only config values checked against a fixed set — an unrecognized
  value in any of them throws a descriptive `Error` naming the bad value
  and nothing is written. `write_disposition` (BigQuery) is not validated
  the same way: any value other than `'append'`/`'truncate'`/etc. is passed
  straight through to `BigQuery.Jobs.insert` as `WRITE_<value>`, which
  BigQuery itself will reject if it's not a real write disposition.
- **Breaking change from the previous shape:** `source.where`/
  `destination.where` used to be `'sql_platform'` with a separate
  `config.platform === 'bigquery'` check — BigQuery was the only platform
  ever implemented, so that indirection is gone; both are now simply
  `where: 'bigquery'`. The Sheets destination's `new_file_flag` (boolean,
  inverted default — `true`-ish unless explicitly `false`) and the
  conditionally-relevant `write_disposition` are replaced by a single
  explicit `mode: 'create' | 'overwrite' | 'append'` (default `'create'`).
  Old-shape payloads (`where: 'sql_platform'`, `new_file_flag`,
  Sheets-destination `write_disposition`) are no longer recognized and will
  throw. `source.config.platform` for `here`/`.sql` files is unchanged —
  still required, still must be `'bigquery'`.
- The Excel-to-temp-Sheet conversion path calls `Drive.Files.remove` after
  the source data has already been read; a failed removal now logs a
  `console.warn` (naming the temp file id and the original `file_id`) and
  still returns the successfully-read data, rather than throwing away good
  data over a cleanup failure. This does **not** cover the script being
  killed between create and remove — no code runs in that case, so the temp
  file still leaks in Drive with no warning.
- `.gs` source files are executed via `eval` on their raw content —
  arbitrary code execution by design, not sandboxed.

## Open questions

- `source.config.platform` for `here`/`.sql` files (required, must be
  `'bigquery'`) has the same "only one real value ever" shape as the
  `sql_platform`/`platform` indirection this change removed elsewhere, but
  wasn't in scope here — left as a candidate for a future pass.
