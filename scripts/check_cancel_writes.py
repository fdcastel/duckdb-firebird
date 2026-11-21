"""An interrupted write cancels the Firebird request it waits on, and keeps nothing.

Four writes that would run for hours, each interrupted after 2 s:
- one firebird_execute statement (an EXECUTE BLOCK that writes a row, then
  computes a four-way cross join of RDB$FIELDS);
- firebird_execute with a source of 100,000,000 rows;
- INSERT INTO an ATTACH table from 100,000,000 rows (DuckDB alone stops it
  between chunks; the case checks the rollback, and is the one that showed
  an unanswered cancel, see FirebirdCancelGuard);
- INSERT INTO an ATTACH table of one row, whose trigger computes that cross
  join (RW_SLOW): the Firebird call itself blocks, in the INSERT's Sink one
  row at a time (Firebird 3) or in its Finalize batch (Firebird 4+).
Each must end with DuckDB's InterruptException within 15 s, its write must
be rolled back, and the session must work afterwards.

Usage: python check_cancel_writes.py <firebird.duckdb_extension> <connection string>
Needs the Python duckdb package of the same version as the extension, and
scripts/fixture_rw.sql loaded in the database.
"""
import os
import sys
import threading
import time

import duckdb

ext, cs = sys.argv[1], sys.argv[2]
SLOW = "SELECT COUNT(*) FROM RDB$FIELDS A, RDB$FIELDS B, RDB$FIELDS C, RDB$FIELDS D"
CASES = [
    ("one long statement", "RW_LOG", -91,
     "SELECT * FROM firebird_execute('rw', 'EXECUTE BLOCK AS DECLARE N BIGINT; BEGIN "
     "INSERT INTO RW_LOG (ID, NOTE) VALUES (-91, ''cancel''); " + SLOW.replace("'", "''") + " INTO :N; END')"),
    ("a source of 100,000,000 rows", "RW_LOG", -92,
     "SELECT * FROM firebird_execute('rw', 'INSERT INTO RW_LOG (ID, NOTE) VALUES (?, ?)', "
     "source := 'SELECT -92, ''cancel'' FROM range(100000000) t(i)')"),
    ("INSERT INTO from 100,000,000 rows", "RW_LOG", -93,
     "INSERT INTO rw.main.RW_LOG SELECT -93, 'cancel' FROM range(100000000) t(i)"),
    ("INSERT INTO a table whose trigger takes hours", "RW_SLOW", -94,
     "INSERT INTO rw.main.RW_SLOW VALUES (-94, 'cancel')"),
]
INTERRUPT_AFTER_S = 2.0
MAX_S = 15.0


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{ext}'")
    con.execute(f"ATTACH '{cs}' AS rw (TYPE firebird, READ_WRITE)")
    return con


def kept(con, table, marker):
    return con.execute(
        f"SELECT * FROM firebird_query('rw', 'SELECT COUNT(*) FROM {table} WHERE ID = {marker}')").fetchall()[0][0]


failed = False
for name, table, marker, sql in CASES:
    con = connect()
    result = {}

    def run():
        t0 = time.monotonic()
        try:
            con.execute(sql).fetchall()
            result["outcome"] = "finished"
        except duckdb.InterruptException:
            result["outcome"] = "interrupted"
        except Exception as e:  # noqa: BLE001 - every other outcome is a failure, reported
            result["outcome"] = f"{type(e).__name__}: {str(e).splitlines()[0][:200]}"
        result["secs"] = time.monotonic() - t0

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    time.sleep(INTERRUPT_AFTER_S)
    con.interrupt()
    worker.join(MAX_S)
    if worker.is_alive():
        print(f"FAIL {name}: still running {MAX_S:.0f} s after the interrupt", flush=True)
        failed = True
        continue
    ok = result["outcome"] == "interrupted"
    print(f"{'ok' if ok else 'FAIL'} {name}: {result['outcome']} after {result['secs']:.1f} s", flush=True)
    failed |= not ok
    n = kept(con, table, marker)
    print(f"{'ok' if n == 0 else 'FAIL'} {name}: {n} row(s) kept", flush=True)
    failed |= n != 0
    con.close()

con = connect()
con.execute("SELECT * FROM firebird_execute('rw', 'DELETE FROM RW_LOG WHERE ID IN (-91, -92, -93)')").fetchall()
con.close()
os._exit(1 if failed else 0)
