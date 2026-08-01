// Entry point for the move() test bed. See spec/test.md for the combination
// matrix this project implements, and CLAUDE.md's "Making and verifying a
// change" section for when to run this vs. example/'s teste().

// Memoized per execution: runAll() calls this once per test function (26x),
// but a single script execution only needs to fetch+eval the library once —
// GAS resets top-level vars between separate executions, so this never
// serves stale code across runs.
var _importedLib_ = null;

function importLib_() {
  if (_importedLib_) return _importedLib_;
  eval(UrlFetchApp.fetch('https://raw.githubusercontent.com/moschionigabriel/tinydeskdata/master/tinydeskdata.js').getContentText());
  _importedLib_ = tinyDeskData;
  return _importedLib_;
}

// Run everything: provision fixtures, then every Sx/Dx/Ix test function.
function runAll() {
  ensureFixtures();
  ensureBqFixtures();

  var fns = [
    testS1_sheetDefault, testS2_sheetNamed, testS3_excel, testS4_csv, testS5_unsupportedMime,
    testS6_hereSql, testS7_hereGs, testS8_hereUnsupportedExt, testS9_bigqueryRead, testS10_unsupportedSourceWhere,
    testD1_sheetsOverwrite, testD2_sheetsAppend, testD3_sheetsNewFile, testD4_sheetsNewFileNamedFolder,
    testD5_csvWrite, testD6_csvNameWithoutExtension, testD7_unsupportedFileType,
    testD8_bigqueryAppendDefault, testD9_bigqueryTruncate, testD10_bigqueryPartitioned, testD11_unsupportedDestinationWhere,
    testD12_sheetsInvalidMode,
    testI1_roundTrip, testI2_writeDispositionAsymmetry, testI3_bigqueryLoadFailureSurfaced,
    testI4_bigquerySchemaMismatchThrowsSynchronously
  ];

  var results = fns.map(function (fn) {
    try {
      fn();
      Logger.log(fn.name + ': PASS');
      return { name: fn.name, status: 'PASS' };
    } catch (e) {
      Logger.log(fn.name + ': FAIL - ' + e);
      return { name: fn.name, status: 'FAIL', error: String(e) };
    }
  });

  var failed = results.filter(function (r) { return r.status === 'FAIL'; });
  Logger.log('---');
  Logger.log(results.length + ' run, ' + failed.length + ' failed');
  if (failed.length) Logger.log('Failed: ' + failed.map(function (r) { return r.name; }).join(', '));
  return results;
}
