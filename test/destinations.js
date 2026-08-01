// One test function per documented move() destination variant (spec/test.md,
// "Destination variants" table). Each reads from the same simple, fully
// deterministic source — the fixture_array.gs.html literal — so the
// assertion is purely about what _moveLoadData did, not about source-read
// quirks.

function hereGsSource_() {
  return { where: 'here', config: { file_name: 'fixture_array.gs' } };
}

function testD1_sheetsOverwrite() {
  var lib = importLib_();
  var sheetId = getFixtureId_('d1_target_sheet');
  lib.move({
    source: hereGsSource_(),
    destination: { where: 'drive', config: { file_type: 'sheets', file_id: sheetId, new_file_flag: false } }
  });
  var values = SpreadsheetApp.openById(sheetId).getSheets()[0].getDataRange().getDisplayValues();
  assertEqual_(values, [['id', 'label'], ['1', 'alpha'], ['2', 'beta']], 'D1 default write_disposition should clear+overwrite from row 1');
}

function testD2_sheetsAppend() {
  var lib = importLib_();
  var sheetId = getFixtureId_('d2_target_sheet');
  lib.move({
    source: hereGsSource_(),
    destination: { where: 'drive', config: { file_type: 'sheets', file_id: sheetId, new_file_flag: false, write_disposition: 'append' } }
  });
  var values = SpreadsheetApp.openById(sheetId).getSheets()[0].getDataRange().getDisplayValues();
  // baseline (ensureFixtures) is [id,label]/[0,seed-row]; append writes the
  // whole source array — including its own header row — starting at row 3.
  assertEqual_(values, [['id', 'label'], ['0', 'seed-row'], ['id', 'label'], ['1', 'alpha'], ['2', 'beta']], 'D2 append should start at getLastRow()+1 and not touch prior rows');
}

function testD3_sheetsNewFile() {
  var lib = importLib_();
  var name = 'd3_new_sheet';
  var existing = DriveApp.getFilesByName(name);
  while (existing.hasNext()) existing.next().setTrashed(true);

  lib.move({
    source: hereGsSource_(),
    destination: { where: 'drive', config: { file_type: 'sheets', file_name: name } }
  });

  var created = DriveApp.getFilesByName(name);
  assert_(created.hasNext(), 'D3 should create a new spreadsheet named ' + name);
  var file = created.next();
  var ss = SpreadsheetApp.openById(file.getId());
  assertEqual_(ss.getSheets()[0].getDataRange().getDisplayValues(), [['id', 'label'], ['1', 'alpha'], ['2', 'beta']], 'D3 new file contents');
  file.setTrashed(true); // clean up — D3's default (no folder_id) lands in Drive root
}

function testD4_sheetsNewFileNamedFolder() {
  var lib = importLib_();
  var name = 'd4_new_sheet';
  var folder = writeTargetsFolder_();
  cleanFilesByName_(folder, name);

  lib.move({
    source: hereGsSource_(),
    destination: { where: 'drive', config: { file_type: 'sheets', file_name: name, sheet_name: 'custom_sheet_name', folder_id: folder.getId() } }
  });

  var found = folder.getFilesByName(name);
  assert_(found.hasNext(), 'D4 should create the new spreadsheet inside folder_id');
  var ss = SpreadsheetApp.openById(found.next().getId());
  assertEqual_(ss.getSheets()[0].getName(), 'custom_sheet_name', 'D4 default sheet should be renamed to sheet_name');
  assertEqual_(ss.getSheets()[0].getDataRange().getDisplayValues(), [['id', 'label'], ['1', 'alpha'], ['2', 'beta']], 'D4 new file contents');
}

function testD5_csvWrite() {
  var lib = importLib_();
  var name = 'd5_output.csv';
  var folder = writeTargetsFolder_();
  cleanFilesByName_(folder, name);

  lib.move({
    source: hereGsSource_(),
    destination: { where: 'drive', config: { file_type: 'csv', file_name: name, folder_id: folder.getId() } }
  });

  var rows = readCsvFile_(folder, name);
  assertEqual_(rows, [['id', 'label'], ['1', 'alpha'], ['2', 'beta']], 'D5 csv contents');
}

function testD6_csvNameWithoutExtension() {
  var lib = importLib_();
  var base = 'd6_output';
  var folder = writeTargetsFolder_();
  cleanFilesByName_(folder, base + '.csv');

  lib.move({
    source: hereGsSource_(),
    destination: { where: 'drive', config: { file_type: 'csv', file_name: base, folder_id: folder.getId() } }
  });

  var found = folder.getFilesByName(base + '.csv');
  assert_(found.hasNext(), 'D6 should append .csv when file_name has no extension');
}

function testD7_unsupportedFileType() {
  var lib = importLib_();
  var folder = writeTargetsFolder_();
  var before = folder.getFiles();
  var beforeCount = 0;
  while (before.hasNext()) { before.next(); beforeCount++; }

  lib.move({
    source: hereGsSource_(),
    destination: { where: 'drive', config: { file_type: 'pdf', file_name: 'd7_output', folder_id: folder.getId() } }
  });

  var after = folder.getFiles();
  var afterCount = 0;
  while (after.hasNext()) { after.next(); afterCount++; }
  assertEqual_(afterCount, beforeCount, 'D7 unrecognized file_type should be a silent no-op — no file created');
}

function testD8_bigqueryAppendDefault() {
  var lib = importLib_();
  var before = bqTableRowCount_('d8_append_target');
  lib.move({
    source: hereGsSource_(),
    destination: { where: 'sql_platform', config: { platform: 'bigquery', credentials: bqCredentials_(), schema_name: BQ_DATASET, table_name: 'd8_append_target' } }
  });
  var after = bqTableRowCount_('d8_append_target');
  assertEqual_(after, before + 2, 'D8 default write_disposition should append (2 data rows added, baseline untouched)');
}

function testD9_bigqueryTruncate() {
  var lib = importLib_();
  lib.move({
    source: hereGsSource_(),
    destination: { where: 'sql_platform', config: { platform: 'bigquery', credentials: bqCredentials_(), schema_name: BQ_DATASET, table_name: 'd9_truncate_target', write_disposition: 'truncate' } }
  });
  var after = bqTableRowCount_('d9_truncate_target');
  assertEqual_(after, 2, 'D9 truncate should replace all rows with just the 2 new ones');
}

function testD10_bigqueryPartitioned() {
  var lib = importLib_();
  dropBqTableIfExists_('d10_partitioned_target');
  lib.move({
    source: { where: 'here', config: { file_name: 'fixture_array_dates.gs' } },
    destination: {
      where: 'sql_platform',
      config: {
        platform: 'bigquery', credentials: bqCredentials_(), schema_name: BQ_DATASET, table_name: 'd10_partitioned_target',
        partition_column: 'event_date'
      }
    }
  });
  var table = BigQuery.Tables.get(BQ_PROJECT_ID, BQ_DATASET, 'd10_partitioned_target');
  var eventDateField = table.schema.fields.filter(function (f) { return f.name === 'event_date'; })[0];
  assertEqual_(eventDateField.type, 'DATE', 'D10 partition_column should be typed DATE');
  var otherFields = table.schema.fields.filter(function (f) { return f.name !== 'event_date'; });
  otherFields.forEach(function (f) { assertEqual_(f.type, 'STRING', 'D10 non-partition columns should stay STRING'); });
  assert_(table.timePartitioning && table.timePartitioning.type === 'DAY' && table.timePartitioning.field === 'event_date', 'D10 should have DAY partitioning on event_date');
}

function testD11_unsupportedDestinationWhere() {
  var lib = importLib_();
  // Just needs to not throw — there's no observable state to assert on for a no-op.
  lib.move({
    source: hereGsSource_(),
    destination: { where: 'nope', config: {} }
  });
}
