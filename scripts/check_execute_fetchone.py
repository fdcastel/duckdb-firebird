"""Writes must commit even when the client reads one row and closes.

A DuckDB statement whose result is streamed commits only once the result
is fully read. A client that read firebird_execute's single row with
fetchone() and closed the connection used to lose every write without an
error (the DuckDB transaction was rolled back, and with it the Firebird
one). firebird_execute now makes DuckDB materialize its result, as DuckDB
does for INSERT, so the statement has run and committed before the client
sees the row.

sqllogictest reads every result fully, so this needs a real client.

Usage: python check_execute_fetchone.py <firebird.duckdb_extension> <connection string>
Needs the Python duckdb package of the same version as the extension, and
scripts/fixture_rw.sql loaded in the database.
"""
import sys

import duckdb

ext, cs = sys.argv[1], sys.argv[2]


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{ext}'")
    con.execute(f"ATTACH '{cs}' AS rw (TYPE firebird, READ_WRITE)")
    return con


def count(marker):
    con = connect()
    n = con.execute(f"SELECT count(*) FROM rw.main.RW_LOG WHERE ID = {marker}").fetchone()[0]
    con.close()
    return n


con = connect()
con.execute("SELECT * FROM firebird_execute('rw', 'DELETE FROM RW_LOG')").fetchall()
con.close()

failures = 0
cases = {
    9001: "SELECT * FROM firebird_execute('rw', 'INSERT INTO RW_LOG (ID, NOTE) VALUES (9001, ''fetchone'')')",
    9002: "SELECT * FROM firebird_execute('rw', 'INSERT INTO RW_LOG (ID, NOTE) VALUES (?, ?)', "
          "source := 'SELECT 9002, ''fetchone''')",
    9003: "INSERT INTO rw.main.RW_LOG VALUES (9003, 'fetchone')",
}
for marker, sql in cases.items():
    con = connect()
    con.execute(sql).fetchone()
    con.close()
    n = count(marker)
    status = "ok" if n == 1 else "FAILED"
    if n != 1:
        failures += 1
    print(f"{status}: {sql[:70]}... -> {n} row(s) after close")

con = connect()
con.execute("SELECT * FROM firebird_execute('rw', 'DELETE FROM RW_LOG')").fetchall()
con.close()

if failures:
    sys.exit(f"check_execute_fetchone: {failures} case(s) lost their writes")
print("check_execute_fetchone: ok")
