"""firebird_query parity / benchmark suite on a synthetic database.

Runs each query of QUERIES three ways and compares them:

  query   firebird_query('fb', <Firebird SQL>)            -- the function under test
  attach  the same query in DuckDB SQL over ATTACHed tables (fb.main.*), where one exists
  odbc    odbc_query(<Firebird ODBC connection>, <Firebird SQL>)   (optional: --odbc)

For every path: best-of-N wall time, rows, and the rows Firebird actually sent (from
firebird_query_log; native paths only). Values are compared against `query` as a multiset
(EXCEPT ALL both ways), raw and again with text right-trimmed (CHAR(n) padding).

Usage (DuckDB 1.5.6; a local build needs allow_unsigned_extensions, set automatically
when --extension is a path):

  uv run --no-project --with duckdb==1.5.6 python scripts/parity/parity.py \
      --database localhost:/data/parity.fdb --create \
      --extension build/release/extension/firebird/firebird.duckdb_extension \
      --odbc --repeat 3 --out parity-results.md

--create builds the fixture with isql (fixture_parity.sql, plus fixture_parity_fb4.sql on
Firebird 4+); the database must not exist yet. Without --create the fixture must already
be loaded. Credentials: --user / --password (default SYSDBA / masterkey).
"""

from __future__ import annotations

import argparse
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

import duckdb

HERE = pathlib.Path(__file__).resolve().parent

# name -> (Firebird SQL, DuckDB SQL over fb.main.* or None, note)
QUERIES: dict[str, tuple[str, str | None, str]] = {
    "join_filter_header": (
        "SELECT i.ID, i.ORDER_ID, o.ISSUED, c.NAME, c.CITY, p.DESCR, i.QTY, i.PRICE, i.DISCOUNT "
        "FROM PAR_ORDER_ITEM i JOIN PAR_ORDER o ON o.ID = i.ORDER_ID "
        "JOIN PAR_CUSTOMER c ON c.ID = o.CUSTOMER_ID JOIN PAR_PRODUCT p ON p.ID = i.PRODUCT_ID "
        "WHERE o.ISSUED >= DATE '2025-07-01' AND o.STATUS = 'F'",
        "SELECT i.ID, i.ORDER_ID, o.ISSUED, c.NAME, c.CITY, p.DESCR, i.QTY, i.PRICE, i.DISCOUNT "
        "FROM fb.main.PAR_ORDER_ITEM i JOIN fb.main.PAR_ORDER o ON o.ID = i.ORDER_ID "
        "JOIN fb.main.PAR_CUSTOMER c ON c.ID = o.CUSTOMER_ID JOIN fb.main.PAR_PRODUCT p ON p.ID = i.PRODUCT_ID "
        "WHERE o.ISSUED >= DATE '2025-07-01' AND o.STATUS = 'F'",
        "detail rows filtered through the header (last 6 months)",
    ),
    "aggregate_month_uf": (
        "SELECT EXTRACT(YEAR FROM o.ISSUED) AS Y, EXTRACT(MONTH FROM o.ISSUED) AS M, c.UF, "
        "COUNT(*) AS N, SUM(i.QTY) AS QTY, SUM(i.QTY * i.PRICE) AS GROSS "
        "FROM PAR_ORDER_ITEM i JOIN PAR_ORDER o ON o.ID = i.ORDER_ID "
        "JOIN PAR_CUSTOMER c ON c.ID = o.CUSTOMER_ID GROUP BY 1, 2, 3",
        "SELECT year(o.ISSUED) AS Y, month(o.ISSUED) AS M, c.UF, "
        "COUNT(*) AS N, SUM(i.QTY) AS QTY, SUM(i.QTY * i.PRICE) AS GROSS "
        "FROM fb.main.PAR_ORDER_ITEM i JOIN fb.main.PAR_ORDER o ON o.ID = i.ORDER_ID "
        "JOIN fb.main.PAR_CUSTOMER c ON c.ID = o.CUSTOMER_ID GROUP BY 1, 2, 3",
        "aggregate over 1 M detail rows",
    ),
    "selectable_procedure": (
        "SELECT * FROM PAR_SP_PRODUCT_SALES",
        None,
        "per-row PSQL work; not reachable through ATTACH",
    ),
    "computed_columns": (
        "SELECT ID, FREIGHT FROM PAR_ORDER_ITEM WHERE ID <= 200000",
        "SELECT ID, FREIGHT FROM fb.main.PAR_ORDER_ITEM WHERE ID <= 200000",
        "correlated COMPUTED BY: server cost, same on every path",
    ),
    "blob_with_nul": (
        "SELECT ID, NOTES FROM PAR_CUSTOMER",
        "SELECT ID, NOTES FROM fb.main.PAR_CUSTOMER",
        "text BLOBs, 50 of them with an embedded NUL byte",
    ),
    "char_padding": (
        "SELECT ID, UNIT FROM PAR_PRODUCT",
        "SELECT ID, UNIT FROM fb.main.PAR_PRODUCT",
        "CHAR(6)",
    ),
    "fb4_types": (
        "SELECT * FROM PAR_FB4",
        "SELECT * FROM fb.main.PAR_FB4",
        "NUMERIC(38,4), DECFLOAT(34), TIME/TIMESTAMP WITH TIME ZONE (Firebird 4+)",
    ),
    "current_timestamp_type": (
        "SELECT CURRENT_TIMESTAMP AS TS, LOCALTIMESTAMP AS LTS FROM RDB$DATABASE",
        None,
        "types only (the values are the time of the run)",
    ),
}
FB4_ONLY = {"fb4_types"}
TYPES_ONLY = {"current_timestamp_type"}


def die(msg: str) -> None:
    print(f"parity.py: {msg}", file=sys.stderr)
    sys.exit(2)


def find_isql(explicit: str | None) -> str:
    if explicit:
        return explicit
    for cand in ("isql-fb", "isql"):
        if shutil.which(cand):
            return shutil.which(cand)  # type: ignore[return-value]
    for base in (r"C:\Program Files\Firebird\Firebird_5_0", r"C:\Program Files\Firebird\Firebird_4_0",
                 r"C:\Program Files\Firebird\Firebird_3_0", "/opt/firebird/bin", "/usr/lib/firebird/bin"):
        for name in ("isql.exe", "isql"):
            p = pathlib.Path(base) / name
            if p.exists():
                return str(p)
    die("isql not found; pass --isql")
    return ""


def isql(isql_path: str, args: argparse.Namespace, script: str, database: str | None = None) -> str:
    cmd = [isql_path, "-q", "-user", args.user, "-password", args.password]
    if database:
        cmd.append(database)
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False, encoding="utf-8") as f:
        f.write(script)
        name = f.name
    try:
        out = subprocess.run(cmd + ["-i", name], capture_output=True, text=True, encoding="utf-8", errors="replace")
    finally:
        os.unlink(name)
    if out.returncode != 0 or "Statement failed" in out.stderr or "SQLSTATE" in out.stderr:
        die(f"isql failed:\n{out.stderr or out.stdout}")
    return out.stdout


def create_fixture(args: argparse.Namespace) -> None:
    exe = find_isql(args.isql)
    print(f"creating {args.database} with {exe} ...", flush=True)
    isql(exe, args, f"CREATE DATABASE '{args.database}' USER '{args.user}' PASSWORD '{args.password}' "
                    "PAGE_SIZE 8192 DEFAULT CHARACTER SET UTF8;\n")
    t0 = time.perf_counter()
    isql(exe, args, (HERE / "fixture_parity.sql").read_text(encoding="utf-8"), args.database)
    version = isql(exe, args, "SET LIST ON; SELECT rdb$get_context('SYSTEM', 'ENGINE_VERSION') AS V FROM rdb$database;\n",
                   args.database)
    major = int(version.split()[-1].split(".")[0])
    if major >= 4:
        isql(exe, args, (HERE / "fixture_parity_fb4.sql").read_text(encoding="utf-8"), args.database)
    print(f"fixture loaded in {time.perf_counter() - t0:.0f} s (engine {version.split()[-1]})", flush=True)


def native_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    local_build = args.extension and os.path.exists(args.extension)
    con = duckdb.connect(config={"allow_unsigned_extensions": True} if local_build else {})
    if local_build:
        con.execute(f"LOAD '{pathlib.Path(args.extension).as_posix()}'")
    else:
        con.execute("INSTALL firebird FROM community")
        con.execute("LOAD firebird")
    con.execute("SET firebird_query_log_size = 100000")
    conn = f"user={args.user};password={args.password};database={args.database};charset=UTF8"
    con.execute(f"ATTACH '{conn}' AS fb (TYPE firebird, READ_ONLY)")
    return con


def odbc_connection(args: argparse.Namespace) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL odbc")
    con.execute("LOAD odbc")
    con.execute("SET threads = 1")  # odbc_scanner is not safe on one connection from several threads
    con.execute("SET VARIABLE c = $1", [f"Driver={{{args.odbc_driver}}};Database={args.database};"
                                        f"UID={args.user};PWD={args.password};CHARSET=UTF8"])
    return con


def log_marker(con: duckdb.DuckDBPyConnection):
    return con.execute("SELECT max(captured_at) FROM firebird_query_log()").fetchone()[0]


def rows_sent(con: duckdb.DuckDBPyConnection, marker) -> int:
    where = "" if marker is None else "WHERE captured_at > $1"
    params = [] if marker is None else [marker]
    return int(con.execute(f"SELECT coalesce(sum(rows_read), 0) FROM firebird_query_log() {where}", params)
               .fetchone()[0])


def timed_copy(con, select_sql: str, out: pathlib.Path, repeat: int, native: bool) -> tuple[float, int, int | None]:
    best, sent = float("inf"), None
    for _ in range(repeat):
        marker = log_marker(con) if native else None
        t0 = time.perf_counter()
        con.execute(f"COPY ({select_sql}) TO '{out.as_posix()}' (FORMAT parquet)")
        best = min(best, time.perf_counter() - t0)
        if native:
            sent = rows_sent(con, marker)
    rows = duckdb.sql(f"SELECT count(*) FROM '{out.as_posix()}'").fetchone()[0]
    return best, rows, sent


def schema(path: pathlib.Path) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in duckdb.sql(f"DESCRIBE SELECT * FROM '{path.as_posix()}'").fetchall()]


def diff(base: pathlib.Path, other: pathlib.Path, trim: bool) -> tuple[int, int]:
    sa, sb = dict(schema(base)), dict(schema(other))
    cols = [c for c in sa if c in sb]

    def proj(types: dict[str, str], other_types: dict[str, str]) -> str:
        out = []
        for c in cols:
            q = f'"{c}"'
            if trim or types[c] != other_types[c]:
                e = f"CAST({q} AS VARCHAR)"
                out.append(f"rtrim({e}) AS {q}" if trim else f"{e} AS {q}")
            else:
                out.append(q)
        return ", ".join(out)

    qa = f"SELECT {proj(sa, sb)} FROM '{base.as_posix()}'"
    qb = f"SELECT {proj(sb, sa)} FROM '{other.as_posix()}'"
    a = duckdb.sql(f"SELECT count(*) FROM ({qa} EXCEPT ALL {qb})").fetchone()[0]
    b = duckdb.sql(f"SELECT count(*) FROM ({qb} EXCEPT ALL {qa})").fetchone()[0]
    return a, b


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--database", required=True, help="host:/path/parity.fdb")
    ap.add_argument("--user", default="SYSDBA")
    ap.add_argument("--password", default="masterkey")
    ap.add_argument("--extension", help="path to firebird.duckdb_extension (default: community build)")
    ap.add_argument("--create", action="store_true", help="create the database and load the fixture first")
    ap.add_argument("--isql", help="path to isql (default: search PATH and the usual install dirs)")
    ap.add_argument("--odbc", action="store_true", help="also run the ODBC path (needs the Firebird ODBC driver)")
    ap.add_argument("--odbc-driver", default="Firebird ODBC Driver")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--only", nargs="*", help="run only these queries")
    ap.add_argument("--out", help="also write the markdown report here")
    args = ap.parse_args()

    if args.create:
        create_fixture(args)

    con = native_connection(args)
    version = con.execute("SELECT * FROM firebird_query('fb', "
                          "'SELECT rdb$get_context(''SYSTEM'', ''ENGINE_VERSION'') FROM rdb$database')").fetchone()[0]
    major = int(str(version).split(".")[0])
    ext_version = con.execute("SELECT extension_version FROM duckdb_extensions() "
                              "WHERE extension_name = 'firebird'").fetchone()[0]
    ocon = odbc_connection(args) if args.odbc else None
    work = pathlib.Path(tempfile.mkdtemp(prefix="parity_"))

    lines = [
        "| query | path | seconds | rows | rows sent by Firebird | value diff vs query (raw, trimmed) | type differences vs query |",
        "|---|---|---:|---:|---:|---|---|",
    ]
    notes = []
    for name, (fb_sql, duck_sql, note) in QUERIES.items():
        if args.only and name not in args.only:
            continue
        if name in FB4_ONLY and major < 4:
            continue
        notes.append(f"- `{name}`: {note}")
        con.execute("SET VARIABLE q = $1", [fb_sql])
        base = work / f"{name}.query.parquet"
        results = {"query": timed_copy(con, "SELECT * FROM firebird_query('fb', getvariable('q'))", base,
                                       args.repeat, native=True)}
        files = {"query": base}
        if duck_sql:
            files["attach"] = work / f"{name}.attach.parquet"
            results["attach"] = timed_copy(con, duck_sql, files["attach"], args.repeat, native=True)
        if ocon is not None:
            ocon.execute("SET VARIABLE q = $1", [fb_sql])
            files["odbc"] = work / f"{name}.odbc.parquet"
            results["odbc"] = timed_copy(ocon, "SELECT * FROM odbc_query(getvariable('c'), getvariable('q'))",
                                         files["odbc"], args.repeat, native=False)
        base_types = dict(schema(base))
        for path, (secs, rows, sent) in results.items():
            if path == "query":
                vd, td = "-", "-"
            else:
                types = dict(schema(files[path]))
                td = "; ".join(f"{c}: {base_types.get(c)} -> {types.get(c)}"
                               for c in base_types if base_types.get(c) != types.get(c)) or "none"
                if name in TYPES_ONLY:
                    vd = "n/a (time of run)"
                else:
                    raw, trimmed = diff(base, files[path], trim=False), diff(base, files[path], trim=True)
                    vd = f"{raw[0]}/{raw[1]}, {trimmed[0]}/{trimmed[1]}"
            lines.append(f"| `{name}` | {path} | {secs:.2f} | {rows:,} | "
                         f"{'' if sent is None else f'{sent:,}'} | {vd} | {td} |")
        print("\n".join(lines[-len(results):]), flush=True)

    header = [
        f"Firebird {version}, DuckDB {duckdb.__version__}, firebird extension {ext_version}, "
        f"best of {args.repeat}" + (f", ODBC driver `{args.odbc_driver}`" if ocon else "") + ".",
        "",
        "Value diff: rows only in `query` / rows only in the other path, compared as a multiset; "
        "\"trimmed\" right-trims every text value first (CHAR padding).",
        "",
    ]
    report = "\n".join(header + lines + ["", "Queries:", ""] + notes) + "\n"
    print("\n" + report)
    if args.out:
        pathlib.Path(args.out).write_text(report, encoding="utf-8")
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
