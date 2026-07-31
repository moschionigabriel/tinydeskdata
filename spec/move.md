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
  - Any other mime type: falls through, `data` stays `undefined`.

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
  - Any other extension: falls through, `data` stays `undefined`.

- **`sql_platform`** — reads an entire existing table: runs
  `select * from {schema_name}.{table_name}` against BigQuery
  (`obj.source.config.credentials.project_id`), polls until complete,
  returns rows with a header row from the schema.

Any `obj.source.where` value not in the above list results in `data` being
`undefined`, and `_moveLoadData` will throw when it tries to read
`data[0].length`.

### Destinations (`obj.destination.where`)

- **`drive`**:
  - `file_type == 'sheets'`:
    - If `new_file_flag == false`: opens an existing spreadsheet by
      `file_id`, or looks it up by `file_name` via
      `DriveApp.getFilesByName(...).next()` if no `file_id` given. Selects
      `sheet_name` or the first sheet.
      - `write_disposition == 'append'`: writes starting at
        `sheet.getLastRow() + 1`.
      - anything else (including omitted): clears all content
        (`clearContent()`) and writes from row 1 — default is
        overwrite/truncate, not append.
    - If `new_file_flag` is not `false` (including omitted/undefined):
      creates a brand new Google Sheet named `file_name`, optionally renames
      the default sheet to `sheet_name`, writes data from row 1, and moves
      the file into `folder_id` if set.
    - Calls `SpreadsheetApp.flush()` at the end.
  - `file_type == 'csv'`: builds a CSV string in memory (quoting fields that
    contain `,`, `"`, or newline, doubling internal quotes), prepends a
    UTF-8 BOM, and creates a file named `file_name` (`.csv` appended if
    missing) in `folder_id` (or the Drive root folder if omitted).

- **`sql_platform`** with `platform == 'bigquery'`:
  - Sanitizes header names to `[a-zA-Z0-9_]` only (all other chars → `_`) to
    derive BigQuery column names.
  - Builds a schema where every column is typed `STRING`, **except** the
    column matching `obj.destination.config.partition_column`, which is
    typed `DATE`.
  - Converts each data row into a JSON object (newline-delimited JSON load
    format) with all values coerced to `String(...)` (or `null`).
  - Loads via `BigQuery.Jobs.insert` with
    `writeDisposition: 'WRITE_' + (write_disposition || 'append').toUpperCase()`
    — default write disposition for BigQuery destinations is **append**,
    unlike Sheets destinations where the default is overwrite.
  - If `partition_column` is set, adds day-partitioning
    (`timePartitioning: { type: 'DAY', field: partition_column }`).
  - Polls `BigQuery.Jobs.get` until `state === 'DONE'`. Does not check
    `status.errorResult` — a failed load job is not surfaced as a thrown
    error here.

Any `obj.destination.where`/`file_type`/`platform` combination not covered
above is silently a no-op — nothing is written and no error is raised.

## Config / Interface

```js
{
  source: {
    where: 'drive' | 'here' | 'sql_platform',
    config: {
      // drive:
      file_id,               // required
      sheet_name,             // optional, defaults to first sheet
      // here:
      parent_folder,          // optional
      file_name,               // required, extension determines handling (.sql | .gs)
      platform,                 // required for .sql, must be 'bigquery'
      credentials: { project_id },
      // sql_platform:
      schema_name, table_name,
      credentials: { project_id },
    }
  },
  destination: {
    where: 'drive' | 'sql_platform',
    config: {
      // drive/sheets:
      file_type: 'sheets' | 'csv',
      file_id,                    // sheets, existing file
      file_name,                  // sheets (lookup/create) or csv
      sheet_name,                 // sheets, optional
      new_file_flag,              // sheets, boolean, default create-new unless explicitly false
      write_disposition,          // sheets: 'append' | anything else = overwrite
      folder_id,                  // sheets (new file) / csv destination folder
      // sql_platform:
      platform: 'bigquery',
      credentials: { project_id },
      schema_name, table_name,
      write_disposition,          // bigquery: 'append' (default) | 'truncate' | ...
      partition_column,           // optional, forces DATE type + day partitioning
    }
  }
}
```

## Edge cases & known limitations

- Unrecognized `source.where`/mime type/extension leaves `data` as
  `undefined`; `_moveLoadData` then throws a raw `TypeError` on
  `data[0].length` rather than a descriptive error.
- Unrecognized `destination.where`/`file_type` is a **silent no-op** — no
  error, no data written. Easy to misconfigure and not notice.
- All source reads use display values (strings), so numeric/date typing is
  lost on the way in; BigQuery destination columns are always `STRING`
  (except the partition column, forced to `DATE`).
- BigQuery load job failures are not checked for `errorResult` — the job
  being "done" is treated as success. A failed load looks like a success to
  the caller.
- Default write behavior differs by destination type: Sheets defaults to
  overwrite, BigQuery defaults to append. Nothing in the config shape makes
  this asymmetry visible.
- The Excel-to-temp-Sheet conversion path assumes `Drive.Files.remove`
  always succeeds; if the script is killed between create and remove, the
  temp file leaks in Drive.
- `.gs` source files are executed via `eval` on their raw content —
  arbitrary code execution by design, not sandboxed.

## Open questions

None currently — this spec describes existing shipped behavior.
