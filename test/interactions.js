// Explicit end-to-end combinations from spec/test.md's "Interaction tests"
// table — cases where the risk is in how source and destination behavior
// combine, not in either stage alone.

function testI1_roundTrip() {
  var lib = importLib_();
  var name = 'i1_out.csv';
  var folder = writeTargetsFolder_();
  cleanFilesByName_(folder, name);

  lib.move({
    source: { where: 'bigquery', config: { schema_name: BQ_DATASET, table_name: 'raw_fixture', credentials: bqCredentials_() } },
    destination: csvDestination_(name)
  });

  var file = folder.getFilesByName(name).next();
  // Apps Script's Blob.getBytes() returns *signed* bytes (-128..127), so
  // compare against the signed equivalents of the UTF-8 BOM (EF BB BF),
  // not the unsigned 0xEF/0xBB/0xBF values.
  var rawBytes = file.getBlob().getBytes().map(function (b) { return b & 0xFF; });
  assert_(rawBytes[0] === 0xEF && rawBytes[1] === 0xBB && rawBytes[2] === 0xBF, 'I1 csv destination should prepend a UTF-8 BOM');

  var rows = readCsvFile_(folder, name);
  assertRowsEqual_(rows, [['id', 'name', 'amount'], ['1', 'Alice', '10.50'], ['2', 'Bob, Jr.', '20.00'], ['3', 'Carla "C"', '30.25']], 'I1 bigquery -> csv round trip contents');
}

function testI2_writeDispositionAsymmetry() {
  var lib = importLib_();
  var sheetId = getFixtureId_('i2_target_sheet');
  var beforeRows = bqTableRowCount_('i2_target_table');

  var payload = {
    source: hereGsSource_(),
    destination: null
  };

  // Same source run against both destination types: Sheets has no implicit
  // "write into an existing file" default anymore, so mode: 'overwrite' is
  // explicit here; BigQuery's write_disposition is left omitted to exercise
  // its own default (append) — the asymmetry being demonstrated is that
  // BigQuery still has an implicit default at all, while Sheets doesn't.
  lib.move(Object.assign({}, payload, {
    destination: { where: 'drive', config: { file_type: 'sheets', file_id: sheetId, mode: 'overwrite' } }
  }));
  lib.move(Object.assign({}, payload, {
    destination: { where: 'bigquery', config: { credentials: bqCredentials_(), schema_name: BQ_DATASET, table_name: 'i2_target_table' } }
  }));

  var sheetValues = SpreadsheetApp.openById(sheetId).getSheets()[0].getDataRange().getDisplayValues();
  assertEqual_(sheetValues, [['id', 'label'], ['1', 'alpha'], ['2', 'beta']], 'I2 sheets destination should have overwritten the baseline row (explicit mode: overwrite)');

  var afterRows = bqTableRowCount_('i2_target_table');
  assertEqual_(afterRows, beforeRows + 2, 'I2 bigquery destination should have appended onto the baseline row (default = append) — same source, only one side has an implicit default');
}

function testI3_bigqueryLoadFailureSurfaced() {
  var lib = importLib_();
  dropBqTableIfExists_('i3_bad_date_target');

  var threw = false;
  var message = '';
  try {
    lib.move({
      // event_date is "not-a-date" — the load schema declares it DATE (it's
      // partition_column), which is a *valid* schema on its own, so
      // BigQuery.Jobs.insert accepts the job. The value can only be
      // rejected once BigQuery actually tries to parse it as a date while
      // processing the row — an async failure, unlike a schema-shape
      // mismatch (column count, or a NOT NULL/mode conflict — both tried
      // first, see spec/test.md's I3 notes — which BigQuery rejects
      // synchronously at submission).
      source: { where: 'here', config: { file_name: 'fixture_bad_date.gs' } },
      destination: {
        where: 'bigquery',
        config: { credentials: bqCredentials_(), schema_name: BQ_DATASET, table_name: 'i3_bad_date_target', partition_column: 'event_date' }
      }
    });
  } catch (e) {
    threw = true;
    message = e.message;
  }

  assert_(threw, 'I3: move() should throw once the underlying BigQuery load job reports errorResult on an invalid DATE value (per spec/move.md)');
  assert_(message.indexOf('i3_bad_date_target') !== -1, 'I3 error message should name the target table, got: ' + message);
  assertEqual_(bqTableRowCountSafe_('i3_bad_date_target'), 0, 'I3: the failed load should not have loaded any rows');
}

function testI4_bigquerySchemaMismatchThrowsSynchronously() {
  var lib = importLib_();
  dropBqTableIfExists_('i4_fixed_schema');
  bqQuery_('create or replace table ' + BQ_DATASET + '.i4_fixed_schema (id STRING, name STRING)');

  var threw = false;
  try {
    lib.move({
      // 3-column result (id, name, amount) against a 2-column existing table.
      // Unlike I3's NOT NULL violation, BigQuery.Jobs.insert rejects this
      // mismatch synchronously — before the job is even created — so it
      // surfaces as a real thrown exception out of move(), not a silently
      // "successful" job. Not documented in spec/move.md before this test
      // bed found it; worth folding back into that spec's edge cases.
      source: { where: 'here', config: { file_name: 'fixture_orders_query.sql', platform: 'bigquery', credentials: bqCredentials_() } },
      destination: { where: 'bigquery', config: { credentials: bqCredentials_(), schema_name: BQ_DATASET, table_name: 'i4_fixed_schema' } }
    });
  } catch (e) {
    threw = true;
  }

  assert_(threw, 'I4: a column-count schema mismatch should throw synchronously from BigQuery.Jobs.insert, unlike I3\'s async NOT NULL violation');
}
