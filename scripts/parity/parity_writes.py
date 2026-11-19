"""Write-path parity and benchmark: odbc_copy vs firebird_execute(source) vs INSERT INTO fb.main.T.

Needs an extension build with the read-write prototype (READ_WRITE attach, firebird_execute,
INSERT INTO fb.main.T). Every case writes into its own table and reads the stored values back
with firebird_query, then compares them with what DuckDB sent (multiset, exact).

Cases:
  sticky_null   NULL / value / NULL / value ... on parameters whose source type differs from the
                column type (INTEGER -> NUMERIC(9,3), INTEGER -> BIGINT, BIGINT -> BIGINT,
                DOUBLE -> NUMERIC(9,3)) -- the shape that the official ODBC driver writes as NULL
                (firebird-odbc-driver issue 300)
  types         one row per value of every common type family, NULLs included
  bulk          --rows rows (default 1,000,000) of INTEGER / DATE / NUMERIC(18,4) / VARCHAR, timed
  floor         --rows executions of a no-op EXECUTE BLOCK (X INTEGER = ?): the per-row cost of
                one round trip, with no server work

usage:
  uv run --no-project --with duckdb==1.5.6 python scripts/parity/parity_writes.py \
      --database localhost:/data/parity.fdb --extension <firebird.duckdb_extension> --odbc
The database must exist (parity.py --create makes one); the cases create their own tables.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import time

import duckdb

STICKY_SQL = """
SELECT i::INTEGER AS id,
       CASE WHEN i % 2 = 0 THEN NULL ELSE (i * 7 % 1000)::INTEGER END AS n93_from_int,
       CASE WHEN i % 2 = 0 THEN NULL ELSE (i * 11)::INTEGER END       AS big_from_int,
       CASE WHEN i % 2 = 0 THEN NULL ELSE (i * 13)::BIGINT END        AS big_from_big,
       CASE WHEN i % 2 = 0 THEN NULL ELSE (i / 4.0)::DOUBLE END       AS n93_from_double
FROM range(1, 2001) t(i)
"""
STICKY_DDL = ("RECREATE TABLE {t} (ID INTEGER NOT NULL, N93_FROM_INT NUMERIC(9,3), BIG_FROM_INT BIGINT, "
              "BIG_FROM_BIG BIGINT, N93_FROM_DOUBLE NUMERIC(9,3))")
STICKY_DML = ("INSERT INTO {t} (ID, N93_FROM_INT, BIG_FROM_INT, BIG_FROM_BIG, N93_FROM_DOUBLE) "
              "VALUES (?, ?, ?, ?, ?)")

TYPES_SQL = """
SELECT * FROM (VALUES
  (1, 1::SMALLINT, 1::INTEGER, 1::BIGINT, 1.5::DECIMAL(9,3), 12345678901234.5678::DECIMAL(18,4),
      1.25::DOUBLE, 'abc', 'X', DATE '1899-12-31', TIME '23:59:59.9999', TIMESTAMP '1900-01-01 00:00:00.0001',
      true, repeat('blob ', 30000)),
  (2, (-32768)::SMALLINT, (-2147483648)::INTEGER, (-9223372036854775807)::BIGINT, (-999999.999)::DECIMAL(9,3),
      (-99999999999999.9999)::DECIMAL(18,4), (-1.0e300)::DOUBLE, 'ação ünï', 'Y', DATE '2026-11-19',
      TIME '00:00:00', TIMESTAMP '2026-11-19 12:34:56.789', false, 'short'),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
) v(id, s, i, b, n93, n184, d, vc, c1, dt, tm, ts, bo, bl)
"""
TYPES_DDL = ("RECREATE TABLE {t} (ID INTEGER NOT NULL, S SMALLINT, I INTEGER, B BIGINT, N93 NUMERIC(9,3), "
             "N184 NUMERIC(18,4), D DOUBLE PRECISION, VC VARCHAR(40), C1 CHAR(1), DT DATE, TM TIME, "
             "TS TIMESTAMP, BO BOOLEAN, BL BLOB SUB_TYPE TEXT)")
TYPES_DML = ("INSERT INTO {t} (ID, S, I, B, N93, N184, D, VC, C1, DT, TM, TS, BO, BL) "
             "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

BULK_SQL = ("SELECT i::INTEGER AS id, DATE '2020-01-01' + (i % 3000)::INTEGER AS d, "
            "(i * 1.2345)::DECIMAL(18,4) AS n, 'row ' || i AS s FROM range({rows}) t(i)")
BULK_DDL = "RECREATE TABLE {t} (ID INTEGER NOT NULL, D DATE, N NUMERIC(18,4), S VARCHAR(60))"
BULK_DML = "INSERT INTO {t} (ID, D, N, S) VALUES (?, ?, ?, ?)"


def connect(args) -> duckdb.DuckDBPyConnection:
    local = args.extension and os.path.exists(args.extension)
    con = duckdb.connect(config={"allow_unsigned_extensions": True} if local else {})
    con.execute(f"LOAD '{pathlib.Path(args.extension).as_posix()}'" if local else "LOAD firebird")
    args.conn = f"user={args.user};password={args.password};database={args.database};charset=UTF8"
    con.execute(f"ATTACH '{args.conn}' AS fb (TYPE firebird, READ_WRITE)")
    if args.odbc:
        con.execute("INSTALL odbc")
        con.execute("LOAD odbc")
        odbc = (f"Driver={{{args.odbc_driver}}};Database={args.database};UID={args.user};"
                f"PWD={args.password};CHARSET=UTF8")
        con.execute("SET VARIABLE odbc_conn = odbc_connect($1)", [odbc])
    return con


def execute(con, sql: str) -> None:
    con.execute("SELECT * FROM firebird_execute('fb', $1)", [sql]).fetchall()


def write(con, path: str, table: str, dml: str, source: str) -> float:
    """One write through *path*; returns the elapsed seconds."""
    t0 = time.perf_counter()
    if path == "odbc_copy":
        con.execute("SET threads = 1")  # odbc_scanner: one connection, one thread
        con.execute("SELECT * FROM odbc_copy(getvariable('odbc_conn'), source_query = $1, dest_query = $2, "
                    "batch_size = 1, max_records_in_transaction = 10000)", [source, dml]).fetchall()
        con.execute("RESET threads")
    elif path == "firebird_execute":
        con.execute("SELECT * FROM firebird_execute('fb', $1, source := $2, commit_every := 10000, "
                    "count_rows := false)", [dml, source]).fetchall()
    else:  # insert_into
        con.execute(f"INSERT INTO fb.main.{table} SELECT * FROM ({source})")
    return time.perf_counter() - t0


def set_batch_size(con, size: int | None) -> bool:
    """SET firebird_write_batch_size (batching experiment); False when the build lacks the setting."""
    if size is None:
        return True
    try:
        con.execute(f"SET firebird_write_batch_size = {int(size)}")
        return True
    except duckdb.Error:
        return False


def verify(con, table: str, source: str) -> tuple[int, int]:
    """(rows missing from the table, rows in the table that were not sent) -- both 0 when exact."""
    got = f"SELECT * FROM firebird_query('fb', 'SELECT * FROM {table}')"
    missing = con.execute(f"SELECT count(*) FROM (({source}) EXCEPT ALL ({got}))").fetchone()[0]
    extra = con.execute(f"SELECT count(*) FROM (({got}) EXCEPT ALL ({source}))").fetchone()[0]
    return missing, extra


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--database", required=True)
    ap.add_argument("--user", default="SYSDBA")
    ap.add_argument("--password", default="masterkey")
    ap.add_argument("--extension")
    ap.add_argument("--odbc", action="store_true")
    ap.add_argument("--odbc-driver", default="Firebird ODBC Driver")
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--batch-sizes", type=int, nargs="*",
                    help="repeat the native paths with SET firebird_write_batch_size = N for each N "
                         "(0 = per-row); needs a build with batching")
    ap.add_argument("--out")
    args = ap.parse_args()

    con = connect(args)
    version = con.execute("SELECT * FROM firebird_query('fb', "
                          "'SELECT rdb$get_context(''SYSTEM'', ''ENGINE_VERSION'') FROM rdb$database')").fetchone()[0]
    # (path, batch size or None). None leaves firebird_write_batch_size alone, which is the
    # only option on a build without batching; --batch-sizes repeats the native paths per size.
    sizes: list[int | None] = list(args.batch_sizes) if args.batch_sizes else [None]
    variants: list[tuple[str, int | None]] = [("odbc_copy", None)] if args.odbc else []
    variants += [(p, b) for b in sizes for p in ("firebird_execute", "insert_into")]
    cases = [
        ("sticky_null", STICKY_DDL, STICKY_DML, STICKY_SQL),
        ("types", TYPES_DDL, TYPES_DML, TYPES_SQL),
        ("bulk", BULK_DDL, BULK_DML, BULK_SQL.format(rows=args.rows)),
    ]
    code = {"odbc_copy": "ODBC", "firebird_execute": "FBX", "insert_into": "INS"}
    lines = ["| case | path | seconds | rows/s | missing | not sent | result |", "|---|---|---:|---:|---:|---:|---|"]
    for case, ddl, dml, source in cases:
        rows = con.execute(f"SELECT count(*) FROM ({source})").fetchone()[0]
        for path, size in variants:
            label = path if size is None else f"{path} (batch {size})"
            if not set_batch_size(con, size):
                lines.append(f"| `{case}` | {label} | | | | | build has no firebird_write_batch_size |")
                print(lines[-1], flush=True)
                continue
            table = f"PARW_{case[:6].upper()}_{code[path]}{'' if size is None else size}"
            execute(con, ddl.format(t=table))
            if path == "insert_into":
                # DDL through firebird_execute doesn't refresh the ATTACH catalog (known gap):
                # re-attach so fb.main sees the table just created.
                con.execute("DETACH fb")
                con.execute(f"ATTACH '{args.conn}' AS fb (TYPE firebird, READ_WRITE)")
            try:
                secs = write(con, path, table, dml.format(t=table), source)
            except duckdb.Error as e:
                con.execute("RESET threads")
                msg = str(e).strip().splitlines()[0][:120].replace("|", "/")
                lines.append(f"| `{case}` | {label} | | | | | error: {msg} |")
                print(lines[-1], flush=True)
                continue
            missing, extra = verify(con, table, source)
            ok = "exact" if (missing, extra) == (0, 0) else "**values differ**"
            lines.append(f"| `{case}` | {label} | {secs:.2f} | {rows / secs:,.0f} | {missing:,} | {extra:,} | {ok} |")
            print(lines[-1], flush=True)

    # Per-row floor: a statement with one parameter and no server work.
    floor = f"SELECT i::INTEGER FROM range({args.rows}) t(i)"
    noop = "EXECUTE BLOCK (X INTEGER = ?) AS BEGIN END"
    for path, size in [v for v in variants if v[0] != "insert_into"]:
        label = path if size is None else f"{path} (batch {size})"
        if not set_batch_size(con, size):
            continue
        try:
            secs = write(con, path, "", noop, floor)
        except duckdb.Error as e:
            con.execute("RESET threads")
            msg = str(e).strip().splitlines()[0][:120].replace("|", "/")
            lines.append(f"| `floor` (no-op EXECUTE BLOCK) | {label} | | | | | error: {msg} |")
            print(lines[-1], flush=True)
            continue
        lines.append(f"| `floor` (no-op EXECUTE BLOCK) | {label} | {secs:.2f} | {args.rows / secs:,.0f} | | | "
                     f"{secs / args.rows * 1e6:.1f} µs/row |")
        print(lines[-1], flush=True)

    header = [f"Firebird {version}, DuckDB {duckdb.__version__}; `missing` = rows sent but not stored as sent, "
              "`not sent` = stored rows that don't match anything sent (both compared as a multiset).", ""]
    report = "\n".join(header + lines) + "\n"
    print("\n" + report)
    if args.out:
        pathlib.Path(args.out).write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
