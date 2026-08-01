// Idempotent (re)creation of the BigQuery-side fixtures move() tests read
// from / write to, in the dedicated tinydeskdata-test project/dataset (kept
// separate from the jaffle-shop-467514 project example/ uses).
//
// Run ensureBqFixtures() (or runAll(), which calls it first) once per
// tinydeskdata-test Apps Script project before running any Sx/Dx/Ix test
// that touches BigQuery.

function ensureBqFixtures() {
  // S6/S9/I1/I3 source: a small existing table to read from / mismatch against.
  bqQuery_(
    'create or replace table ' + BQ_DATASET + '.raw_fixture as ' +
    'select * from unnest([' +
    'struct("1" as id, "Alice" as name, "10.50" as amount), ' +
    'struct("2" as id, "Bob, Jr." as name, "20.00" as amount), ' +
    'struct("3" as id, "Carla \\"C\\"" as name, "30.25" as amount)' +
    '])'
  );

  // I3: dropped rather than seeded — move() relies on default
  // createDisposition (CREATE_IF_NEEDED), and the failure this test needs
  // (an invalid DATE value) only surfaces once BigQuery actually processes
  // row data, regardless of whether the table pre-existed. Two earlier
  // attempts (column-count mismatch, then a NOT NULL constraint) both
  // turned out to be rejected *synchronously* by BigQuery.Jobs.insert's own
  // schema-compatibility check — see spec/test.md's I3 notes.
  dropBqTableIfExists_('i3_bad_date_target');

  // I4: a 2-column table that fixture_orders_query.sql.html's 3-column
  // result doesn't match — this is the column-count-mismatch case that
  // turned out to throw synchronously (see I3's comment above).
  bqQuery_('create or replace table ' + BQ_DATASET + '.i4_fixed_schema (id STRING, name STRING)');

  // D8: append-default destination, seeded with one baseline row.
  bqQuery_(
    'create or replace table ' + BQ_DATASET + '.d8_append_target as ' +
    'select * from unnest([struct("0" as id, "seed-row" as label)])'
  );

  // D9: truncate destination, seeded with two baseline rows that should be gone after the test.
  bqQuery_(
    'create or replace table ' + BQ_DATASET + '.d9_truncate_target as ' +
    'select * from unnest([struct("0" as id, "seed-row-a" as label), struct("00" as id, "seed-row-b" as label)])'
  );

  // I2: its own append-default target, kept separate from D8's so I2 stays
  // self-contained and order-independent.
  bqQuery_(
    'create or replace table ' + BQ_DATASET + '.i2_target_table as ' +
    'select * from unnest([struct("0" as id, "seed-row" as label)])'
  );

  // D10: partitioned destination — dropped rather than seeded, since move()
  // relies on BigQuery's default createDisposition (CREATE_IF_NEEDED) to
  // create it fresh with day partitioning on event_date.
  dropBqTableIfExists_('d10_partitioned_target');

  Logger.log('BigQuery fixtures ready in ' + BQ_PROJECT_ID + '.' + BQ_DATASET);
}
