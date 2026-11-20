"""Fork-only investigation (issue 36): prints results, asserts nothing.

1. WIN1252 text in a UTF8 database: the same text written as a literal in the
   SQL (the reference), as a parameter (source), through INSERT INTO, and in a
   batch. Measured on the server: stored bytes and equality with the literal.
2. Wide DECIMALs into NUMERIC(18,4): the literal in the SQL text against the
   same value as a parameter, per row and in a batch.

Usage: python investigate_36.py <firebird.duckdb_extension> <connection string>
"""
import json
import sys

import duckdb

ext, cs = sys.argv[1], sys.argv[2]
TEXT = "Ação"


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{ext}'")
    con.execute(f"ATTACH '{cs}' AS rw (TYPE firebird, READ_WRITE)")
    return con


def show(label, fn):
    try:
        print(json.dumps({"case": label, "result": fn()}, ensure_ascii=False, default=str), flush=True)
    except Exception as e:  # noqa: BLE001 - an investigation reports every outcome
        print(json.dumps({"case": label, "error": str(e).splitlines()[0][:300]}, ensure_ascii=False), flush=True)


def fx(con, sql, **kw):
    args = "".join(f", {k} := {v}" for k, v in kw.items())
    return con.execute(f"SELECT * FROM firebird_execute('rw', $1{args})", [sql]).fetchall()


con = connect()
show("engine", lambda: con.execute(
    "SELECT * FROM firebird_query('rw', 'SELECT RDB$GET_CONTEXT(''SYSTEM'', ''ENGINE_VERSION'') FROM RDB$DATABASE')").fetchall())
show("duckdb", lambda: duckdb.__version__)
show("ddl", lambda: (fx(con, "RECREATE TABLE RW_CS1252 (ID INTEGER NOT NULL PRIMARY KEY, "
                             "V VARCHAR(10) CHARACTER SET WIN1252, B BLOB SUB_TYPE TEXT CHARACTER SET WIN1252)"),
                     fx(con, "RECREATE TABLE RW_CS1252V (ID INTEGER NOT NULL PRIMARY KEY, "
                             "V VARCHAR(10) CHARACTER SET WIN1252)")))
con.close()

# --- 1. WIN1252 ---------------------------------------------------------------
con = connect()
show("1 literal", lambda: fx(con, f"INSERT INTO RW_CS1252 (ID, V, B) VALUES (1, '{TEXT}', '{TEXT}')"))
show("2 source param", lambda: fx(con, "INSERT INTO RW_CS1252 (ID, V, B) VALUES (?, ?, ?)",
                                  source=f"'SELECT 2, ''{TEXT}'', ''{TEXT}'''"))
show("3 INSERT INTO", lambda: con.execute(f"INSERT INTO rw.main.RW_CS1252 VALUES (3, '{TEXT}', '{TEXT}')").fetchall())
show("4 literal (varchar)", lambda: fx(con, f"INSERT INTO RW_CS1252V (ID, V) VALUES (1, '{TEXT}')"))
show("5 source batch (varchar)", lambda: fx(con, "INSERT INTO RW_CS1252V (ID, V) VALUES (?, ?)",
                                            source=f"'SELECT 2, ''{TEXT}'''", batch_size=100))
show("5 write_mode", lambda: con.execute("SELECT write_mode FROM firebird_last_query()").fetchall())
show("6 source per row (varchar)", lambda: fx(con, "INSERT INTO RW_CS1252V (ID, V) VALUES (?, ?)",
                                              source=f"'SELECT 3, ''{TEXT}'''", batch_size=0))
con.close()

con = connect()
show("server bytes RW_CS1252 (id, octets V, octets B, chars B, V = literal, B = literal)", lambda: con.execute(
    "SELECT * FROM firebird_query('rw', $$SELECT ID, OCTET_LENGTH(V), OCTET_LENGTH(B), CHAR_LENGTH(B), "
    f"IIF(V = '{TEXT}', 1, 0), IIF(CAST(B AS VARCHAR(20) CHARACTER SET WIN1252) = '{TEXT}', 1, 0) "
    "FROM RW_CS1252 ORDER BY ID$$)").fetchall())
show("server bytes RW_CS1252V (id, octets V, V = literal)", lambda: con.execute(
    "SELECT * FROM firebird_query('rw', $$SELECT ID, OCTET_LENGTH(V), "
    f"IIF(V = '{TEXT}', 1, 0) FROM RW_CS1252V ORDER BY ID$$)").fetchall())
show("read back via firebird_query", lambda: con.execute(
    "SELECT * FROM firebird_query('rw', 'SELECT ID, V, B FROM RW_CS1252 ORDER BY ID')").fetchall())
show("read back via scan", lambda: con.execute("SELECT ID, V, B FROM rw.main.RW_CS1252 ORDER BY ID").fetchall())
con.close()

# --- 2. Wide DECIMALs into NUMERIC(18,4) -----------------------------------------
CASES = [
    ("1.12345678901234567891", "DECIMAL(38,20)"),
    ("123456789.12345678901234", "DECIMAL(38,14)"),
    ("0.00005000000000000000", "DECIMAL(38,20)"),
    ("-1.00004999999999999999", "DECIMAL(38,20)"),
    ("922337203685477.58075000", "DECIMAL(38,8)"),
]
con = connect()
show("cleanup", lambda: fx(con, "DELETE FROM RW_TARGET WHERE ID >= 9000"))
for i, (lit, typ) in enumerate(CASES):
    show(f"dec {i} literal {lit}", lambda: fx(con, f"INSERT INTO RW_TARGET (ID, C_NUM18_4) VALUES ({9100 + i}, {lit})"))
    show(f"dec {i} param per row {lit}::{typ}", lambda: fx(
        con, "INSERT INTO RW_TARGET (ID, C_NUM18_4) VALUES (?, ?)",
        source=f"'SELECT {9200 + i}, {lit}::{typ}'", batch_size=0))
    show(f"dec {i} param batch {lit}::{typ}", lambda: fx(
        con, "INSERT INTO RW_TARGET (ID, C_NUM18_4) VALUES (?, ?)",
        source=f"'SELECT {9300 + i}, {lit}::{typ}'", batch_size=100))
show("dec stored (id, value as text)", lambda: con.execute(
    "SELECT * FROM firebird_query('rw', 'SELECT ID, CAST(C_NUM18_4 AS VARCHAR(30)) FROM RW_TARGET "
    "WHERE ID >= 9000 ORDER BY MOD(ID, 100), ID')").fetchall())
show("cleanup", lambda: fx(con, "DELETE FROM RW_TARGET WHERE ID >= 9000"))
con.close()
