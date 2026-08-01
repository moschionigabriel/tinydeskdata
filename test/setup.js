// One-time (idempotent) provisioning of the Drive fixtures move() tests read
// from / write to. Safe to re-run: reuses existing folders/files by name,
// and resets the mutable D1/D2 target sheets to a known baseline each time
// so overwrite/append assertions stay deterministic across runs.
//
// Run ensureFixtures() (or runAll(), which calls it first) once per
// tinydeskdata-test Apps Script project before running any Sx/Dx/Ix test.
// (XLSX_FIXTURE_B64 lives in xlsx_fixture.js.)

function getOrCreateFolder_(name, parent) {
  var it = (parent || DriveApp.getRootFolder()).getFoldersByName(name);
  if (it.hasNext()) return it.next();
  return (parent || DriveApp.getRootFolder()).createFolder(name);
}

function getOrCreateFileInFolder_(folder, name, createFn) {
  var it = folder.getFilesByName(name);
  if (it.hasNext()) return it.next();
  return createFn();
}

function ensureFixtures() {
  var fixturesFolder = getOrCreateFolder_(FIXTURES_FOLDER_NAME);
  var writeTargetsFolder = getOrCreateFolder_(WRITE_TARGETS_FOLDER_NAME, fixturesFolder);
  setFixtureId_('fixtures_folder', fixturesFolder.getId());
  setFixtureId_('write_targets_folder', writeTargetsFolder.getId());

  // S1/S2: a Sheet with a default sheet + a named second sheet.
  var sheetFile = getOrCreateFileInFolder_(fixturesFolder, 'move_test_source_sheet', function () {
    var ss = SpreadsheetApp.create('move_test_source_sheet');
    DriveApp.getFileById(ss.getId()).moveTo(fixturesFolder);
    return DriveApp.getFileById(ss.getId());
  });
  var ss = SpreadsheetApp.openById(sheetFile.getId());
  var defaultSheet = ss.getSheets()[0];
  defaultSheet.clearContents();
  defaultSheet.getRange(1, 1, 3, 3).setValues([
    ['id', 'name', 'amount'],
    ['1', 'Alice', '10.50'],
    ['2', 'Bob', '20.00']
  ]);
  var namedSheet = ss.getSheetByName('second_sheet') || ss.insertSheet('second_sheet');
  namedSheet.clearContents();
  namedSheet.getRange(1, 1, 2, 2).setValues([
    ['id', 'label'],
    ['9', 'from_second_sheet']
  ]);
  setFixtureId_('sheet_fixture', sheetFile.getId());

  // S4: a CSV with a quoting edge case (comma + embedded quote).
  var csvContent = 'id,name,amount\n1,Alice,10.50\n2,"Bob, Jr.",20.00\n3,"Carla ""C""",30.25\n';
  var csvFile = getOrCreateFileInFolder_(fixturesFolder, 'move_test_source.csv', function () {
    return fixturesFolder.createFile('move_test_source.csv', csvContent, MimeType.CSV);
  });
  setFixtureId_('csv_fixture', csvFile.getId());

  // S5: an unsupported mime type for the drive source (plain text).
  var txtFile = getOrCreateFileInFolder_(fixturesFolder, 'move_test_unsupported.txt', function () {
    return fixturesFolder.createFile('move_test_unsupported.txt', 'not a spreadsheet, csv, or excel file', MimeType.PLAIN_TEXT);
  });
  setFixtureId_('txt_fixture', txtFile.getId());

  // S3: a real .xlsx binary (built with openpyxl, embedded as base64 in xlsx_fixture.js).
  var xlsxFile = getOrCreateFileInFolder_(fixturesFolder, 'move_test_source.xlsx', function () {
    var blob = Utilities.newBlob(Utilities.base64Decode(XLSX_FIXTURE_B64),
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'move_test_source.xlsx');
    return fixturesFolder.createFile(blob);
  });
  setFixtureId_('xlsx_fixture', xlsxFile.getId());

  // D1/D2: dedicated target sheets, reset to a known baseline every run so
  // overwrite/append assertions stay deterministic across repeated runs.
  var d1Sheet = getOrCreateFileInFolder_(fixturesFolder, 'd1_target_sheet', function () {
    var s = SpreadsheetApp.create('d1_target_sheet');
    DriveApp.getFileById(s.getId()).moveTo(fixturesFolder);
    return DriveApp.getFileById(s.getId());
  });
  var d1ss = SpreadsheetApp.openById(d1Sheet.getId()).getSheets()[0];
  d1ss.clearContents();
  d1ss.getRange(1, 1, 2, 2).setValues([['stale', 'baseline'], ['should', 'be-overwritten']]);
  setFixtureId_('d1_target_sheet', d1Sheet.getId());

  var d2Sheet = getOrCreateFileInFolder_(fixturesFolder, 'd2_target_sheet', function () {
    var s = SpreadsheetApp.create('d2_target_sheet');
    DriveApp.getFileById(s.getId()).moveTo(fixturesFolder);
    return DriveApp.getFileById(s.getId());
  });
  var d2ss = SpreadsheetApp.openById(d2Sheet.getId()).getSheets()[0];
  d2ss.clearContents();
  d2ss.getRange(1, 1, 2, 2).setValues([['id', 'label'], ['0', 'seed-row']]);
  setFixtureId_('d2_target_sheet', d2Sheet.getId());

  // I2: its own target sheet (same baseline shape as d2), kept separate from
  // D1/D2's fixtures so I2 stays self-contained and order-independent.
  var i2Sheet = getOrCreateFileInFolder_(fixturesFolder, 'i2_target_sheet', function () {
    var s = SpreadsheetApp.create('i2_target_sheet');
    DriveApp.getFileById(s.getId()).moveTo(fixturesFolder);
    return DriveApp.getFileById(s.getId());
  });
  var i2ss = SpreadsheetApp.openById(i2Sheet.getId()).getSheets()[0];
  i2ss.clearContents();
  i2ss.getRange(1, 1, 2, 2).setValues([['id', 'label'], ['0', 'seed-row']]);
  setFixtureId_('i2_target_sheet', i2Sheet.getId());

  Logger.log('Fixtures ready under folder ' + fixturesFolder.getUrl());
}
