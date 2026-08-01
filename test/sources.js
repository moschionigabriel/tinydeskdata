// One test function per documented move() source variant (spec/test.md,
// "Source variants" table). Each writes to the same kind of destination —
// a CSV in the write-targets folder — so the assertion is purely about what
// _moveGetData produced, not about destination-write quirks.

function writeTargetsFolder_() {
  return DriveApp.getFolderById(getFixtureId_('write_targets_folder'));
}

function csvDestination_(fileName) {
  return { where: 'drive', config: { file_type: 'csv', file_name: fileName, folder_id: getFixtureId_('write_targets_folder') } };
}

function testS1_sheetDefault() {
  var lib = importLib_();
  var name = 's1_out.csv';
  cleanFilesByName_(writeTargetsFolder_(), name);
  lib.move({
    source: { where: 'drive', config: { file_id: getFixtureId_('sheet_fixture') } },
    destination: csvDestination_(name)
  });
  var rows = readCsvFile_(writeTargetsFolder_(), name);
  // Sheets auto-detects numeric-looking cell input on write: "10.50"/"20.00"
  // become real number cells, which then *display* (and round-trip through
  // move()'s getDisplayValues() read) as "10.5"/"20" — trailing zeros lost.
  // See spec/move.md's edge cases.
  assertEqual_(rows, [['id', 'name', 'amount'], ['1', 'Alice', '10.5'], ['2', 'Bob', '20']], 'S1 default sheet contents');
}

function testS2_sheetNamed() {
  var lib = importLib_();
  var name = 's2_out.csv';
  cleanFilesByName_(writeTargetsFolder_(), name);
  lib.move({
    source: { where: 'drive', config: { file_id: getFixtureId_('sheet_fixture'), sheet_name: 'second_sheet' } },
    destination: csvDestination_(name)
  });
  var rows = readCsvFile_(writeTargetsFolder_(), name);
  assertEqual_(rows, [['id', 'label'], ['9', 'from_second_sheet']], 'S2 named sheet contents');
}

function testS3_excel() {
  var lib = importLib_();
  var name = 's3_out.csv';
  cleanFilesByName_(writeTargetsFolder_(), name);
  var before = DriveApp.getFilesByName('temp_tdp');
  var beforeCount = 0;
  while (before.hasNext()) { before.next(); beforeCount++; }

  lib.move({
    source: { where: 'drive', config: { file_id: getFixtureId_('xlsx_fixture') } },
    destination: csvDestination_(name)
  });

  var rows = readCsvFile_(writeTargetsFolder_(), name);
  assertEqual_(rows, [['id', 'name', 'amount'], ['1', 'Alice', '10.50'], ['2', 'Bob, Jr.', '20.00'], ['3', 'Carla "C"', '30.25']], 'S3 excel contents');

  var after = DriveApp.getFilesByName('temp_tdp');
  var afterCount = 0;
  while (after.hasNext()) { after.next(); afterCount++; }
  assertEqual_(afterCount, beforeCount, 'S3 temp conversion file should be cleaned up (Drive.Files.remove)');
}

function testS4_csv() {
  var lib = importLib_();
  var name = 's4_out.csv';
  cleanFilesByName_(writeTargetsFolder_(), name);
  lib.move({
    source: { where: 'drive', config: { file_id: getFixtureId_('csv_fixture') } },
    destination: csvDestination_(name)
  });
  var rows = readCsvFile_(writeTargetsFolder_(), name);
  assertEqual_(rows, [['id', 'name', 'amount'], ['1', 'Alice', '10.50'], ['2', 'Bob, Jr.', '20.00'], ['3', 'Carla "C"', '30.25']], 'S4 csv round trip');
}

function testS5_unsupportedMime() {
  var lib = importLib_();
  var name = 's5_out.csv';
  cleanFilesByName_(writeTargetsFolder_(), name);
  var threw = false;
  var message = '';
  try {
    lib.move({
      source: { where: 'drive', config: { file_id: getFixtureId_('txt_fixture') } },
      destination: csvDestination_(name)
    });
  } catch (e) {
    threw = true;
    message = e.message;
  }
  assert_(threw, 'S5 unrecognized mime type should throw a descriptive Error, per spec/move.md');
  assert_(message.indexOf('text/plain') !== -1, 'S5 error message should name the unsupported mimeType, got: ' + message);
}

function testS6_hereSql() {
  var lib = importLib_();
  var name = 's6_out.csv';
  cleanFilesByName_(writeTargetsFolder_(), name);
  lib.move({
    source: { where: 'here', config: { file_name: 'fixture_orders_query.sql', platform: 'bigquery', credentials: bqCredentials_() } },
    destination: csvDestination_(name)
  });
  var rows = readCsvFile_(writeTargetsFolder_(), name);
  assertEqual_(rows, [['id', 'name', 'amount'], ['1', 'Alice', '10.50'], ['2', 'Bob, Jr.', '20.00'], ['3', 'Carla "C"', '30.25']], 'S6 here/.sql query result');
}

function testS7_hereGs() {
  var lib = importLib_();
  var name = 's7_out.csv';
  cleanFilesByName_(writeTargetsFolder_(), name);
  lib.move({
    source: { where: 'here', config: { file_name: 'fixture_array.gs' } },
    destination: csvDestination_(name)
  });
  var rows = readCsvFile_(writeTargetsFolder_(), name);
  assertEqual_(rows, [['id', 'label'], ['1', 'alpha'], ['2', 'beta']], 'S7 here/.gs literal');
}

function testS8_hereUnsupportedExt() {
  var lib = importLib_();
  var name = 's8_out.csv';
  cleanFilesByName_(writeTargetsFolder_(), name);
  var threw = false;
  var message = '';
  try {
    lib.move({
      source: { where: 'here', config: { file_name: 'fixture_unused.json' } },
      destination: csvDestination_(name)
    });
  } catch (e) {
    threw = true;
    message = e.message;
  }
  assert_(threw, 'S8 unrecognized here/ extension should throw a descriptive Error, same as S5');
  assert_(message.indexOf('json') !== -1, 'S8 error message should name the unsupported extension, got: ' + message);
}

function testS9_bigqueryRead() {
  var lib = importLib_();
  var name = 's9_out.csv';
  cleanFilesByName_(writeTargetsFolder_(), name);
  lib.move({
    source: { where: 'bigquery', config: { schema_name: BQ_DATASET, table_name: 'raw_fixture', credentials: bqCredentials_() } },
    destination: csvDestination_(name)
  });
  var rows = readCsvFile_(writeTargetsFolder_(), name);
  assertRowsEqual_(rows, [['id', 'name', 'amount'], ['1', 'Alice', '10.50'], ['2', 'Bob, Jr.', '20.00'], ['3', 'Carla "C"', '30.25']], 'S9 bigquery table read');
}

function testS10_unsupportedSourceWhere() {
  var lib = importLib_();
  var name = 's10_out.csv';
  cleanFilesByName_(writeTargetsFolder_(), name);
  var threw = false;
  var message = '';
  try {
    lib.move({
      source: { where: 'nope', config: {} },
      destination: csvDestination_(name)
    });
  } catch (e) {
    threw = true;
    message = e.message;
  }
  assert_(threw, 'S10 unrecognized source.where should throw a descriptive Error, same as S5/S8');
  assert_(message.indexOf('nope') !== -1, 'S10 error message should name the unrecognized source.where, got: ' + message);
}
