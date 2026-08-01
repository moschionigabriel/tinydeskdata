// Shared constants + small helpers for the move() test bed.
// See spec/test.md for the combination matrix these files implement.

var BQ_PROJECT_ID = 'tinydeskdata-test';
var BQ_DATASET = 'tinydeskdata_test';

var FIXTURES_FOLDER_NAME = 'tinydeskdata-test-fixtures';
var WRITE_TARGETS_FOLDER_NAME = 'write-targets';

function bqCredentials_() {
  return { project_id: BQ_PROJECT_ID };
}

function propKey_(key) {
  return 'tdd_test_' + key;
}

function getFixtureId_(key) {
  var id = PropertiesService.getScriptProperties().getProperty(propKey_(key));
  if (!id) throw new Error('Fixture "' + key + '" not set up yet — run ensureFixtures() (or runAll()) first.');
  return id;
}

function setFixtureId_(key, id) {
  PropertiesService.getScriptProperties().setProperty(propKey_(key), id);
}

function assert_(cond, message) {
  if (!cond) throw new Error('assertion failed: ' + message);
}

function assertEqual_(actual, expected, message) {
  var a = JSON.stringify(actual);
  var e = JSON.stringify(expected);
  if (a !== e) throw new Error((message || 'values differ') + ' — expected ' + e + ', got ' + a);
}

// For asserting on rows read via sql_platform: move()'s own query is a bare
// `select * from schema.table` with no ORDER BY (see spec/move.md), so
// BigQuery is free to return rows in any order. Sort by the header row +
// first column so comparisons are order-independent but still catch actual
// content differences.
function assertRowsEqual_(actual, expected, message) {
  var sortBody = function (rows) {
    var header = rows[0];
    var body = rows.slice(1).slice().sort(function (a, b) { return JSON.stringify(a) < JSON.stringify(b) ? -1 : 1; });
    return [header].concat(body);
  };
  assertEqual_(sortBody(actual), sortBody(expected), message);
}

function bqQuery_(query) {
  var result = BigQuery.Jobs.query({ query: query, useLegacySql: false }, BQ_PROJECT_ID);
  while (!result.jobComplete) {
    Utilities.sleep(500);
    result = BigQuery.Jobs.getQueryResults(BQ_PROJECT_ID, result.jobReference.jobId);
  }
  var rows = (result.rows || []).map(function (row) { return row.f.map(function (cell) { return cell.v; }); });
  var headers = result.schema.fields.map(function (f) { return f.name; });
  return { headers: headers, rows: rows };
}

function bqTableRowCount_(tableName) {
  return Number(bqQuery_('select count(*) as n from ' + BQ_DATASET + '.' + tableName).rows[0][0]);
}

// Like bqTableRowCount_, but treats a missing table as 0 rows instead of
// throwing — for asserting "nothing landed" after a load that may have
// failed before the destination table was even created.
function bqTableRowCountSafe_(tableName) {
  try {
    return bqTableRowCount_(tableName);
  } catch (e) {
    return 0;
  }
}

function dropBqTableIfExists_(tableName) {
  try {
    BigQuery.Tables.remove(BQ_PROJECT_ID, BQ_DATASET, tableName);
  } catch (e) {
    // table didn't exist — fine, that's the desired end state.
  }
}

// Trashes any existing file(s) with this name in folder, so a test that
// creates a file by name can assume a clean slate on every run.
function cleanFilesByName_(folder, name) {
  var it = folder.getFilesByName(name);
  while (it.hasNext()) it.next().setTrashed(true);
}

function readCsvFile_(folder, name) {
  var it = folder.getFilesByName(name);
  if (!it.hasNext()) throw new Error('expected CSV file "' + name + '" to exist in ' + folder.getName());
  var text = it.next().getBlob().getDataAsString('utf-8').replace(/^\ufeff/, '');
  return Utilities.parseCsv(text);
}
