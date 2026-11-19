# firebird_query parity and benchmark suite

A synthetic Firebird database, a set of extraction queries, and a runner that executes each query up to three ways and compares them:

| Path | What runs |
|---|---|
| `query` | `firebird_query('fb', <Firebird SQL>)`, the function under test |
| `attach` | the same query written in DuckDB SQL over the ATTACHed tables (`fb.main.*`), where one can be written |
| `odbc` | `odbc_query(<Firebird ODBC connection>, <Firebird SQL>)`, the usual alternative today (optional) |

For each path it reports:
- best-of-N wall time;
- rows;
- **rows Firebird actually sent**, from `firebird_query_log()` (native paths only);
- the type differences;
- the value differences, as a multiset compared against `query` (raw, and again with text right-trimmed for CHAR padding).

Everything is synthetic and deterministic (`fixture_parity.sql`, plus `fixture_parity_fb4.sql` on Firebird 4+).

## Run

```sh
# DuckDB 1.5.6; a local build is loaded with allow_unsigned_extensions automatically
uv run --no-project --with duckdb==1.5.6 python scripts/parity/parity.py \
    --database localhost:/data/parity.fdb --create \
    --extension build/release/extension/firebird/firebird.duckdb_extension \
    --odbc --repeat 3 --out parity-results.md
```

- `--create` makes the database and loads the fixture with `isql`; on Firebird 5 that takes about 15 s.
- `--odbc` needs the Firebird ODBC driver.
- `--only <query> ...` runs a subset.

## The fixture

| Object | Rows | Shape |
|---|---:|---|
| `PAR_CUSTOMER` | 50,000 | `NOTES` text BLOB (50 rows contain an embedded NUL byte), `UF CHAR(2)` |
| `PAR_PRODUCT` | 20,000 | `UNIT CHAR(6)`, `MARGIN` computed column |
| `PAR_ORDER` | 200,000 | header, `ISSUED` indexed, dates 2021–2025 |
| `PAR_ORDER_ITEM` | 1,000,000 | detail; `FREIGHT` is a correlated `COMPUTED BY` subquery |
| `PAR_SP_PRODUCT_SALES` | 20,000 out | selectable procedure, two lookups per product |
| `PAR_FB4` (Firebird 4+) | 10,000 | `NUMERIC(38,4)`, `DECFLOAT(34)`, `TIMESTAMP` / `TIME WITH TIME ZONE` |

## Results: Firebird 5.0.4, Windows, server on the same host

Setup: DuckDB 1.5.6, extension a400738 (branch `read-write`), official Firebird ODBC driver 3.0.1.21, best of 3.

| query | path | seconds | rows | rows sent by Firebird | value diff vs `query` (raw, trimmed) | type differences vs `query` |
|---|---|---:|---:|---:|---|---|
| `join_filter_header` | query | 0.74 | 90,680 | 90,680 | - | - |
| | attach | 2.65 | 90,680 | **1,087,769** | 0/0, 0/0 | none |
| | odbc | 0.78 | 90,680 | | 0/0, 0/0 | none |
| `aggregate_month_uf` | query | 2.13 | 180 | **180** | - | - |
| | attach | 1.84 | 180 | **1,250,000** | 0/0, 0/0 | Y, M: SMALLINT → BIGINT; QTY: DECIMAL(18,3) → DECIMAL(38,3) |
| | odbc | 2.07 | 180 | | 0/0, 0/0 | GROSS: DECIMAL(38,7) → **VARCHAR** |
| `selectable_procedure` | query | 3.20 | 20,000 | 20,000 | - | - |
| | odbc | 3.18 | 20,000 | | 0/0, 0/0 | none |
| `computed_columns` | query | 0.64 | 200,000 | 200,000 | - | - |
| | attach | 0.63 | 200,000 | 200,000 | 0/0, 0/0 | none |
| | odbc | 0.71 | 200,000 | | 0/0, 0/0 | FREIGHT: DECIMAL(38,4) → **VARCHAR** |
| `blob_with_nul` | query | 0.26 | 50,000 | 50,000 | - | - |
| | attach | 0.24 | 50,000 | 50,000 | 0/0, 0/0 | none |
| | odbc | 0.21 | 50,000 | | **50/50**, 50/50 | none |
| `char_padding` | query | 0.04 | 20,000 | 20,000 | - | - |
| | attach | 0.04 | 20,000 | 20,000 | 0/0, 0/0 | none |
| | odbc | 0.05 | 20,000 | | 20000/20000, **0/0** | none |
| `fb4_types` | query | 0.03 | 10,000 | 10,000 | - | - |
| | attach | 0.03 | 10,000 | 10,000 | 0/0, 0/0 | none |
| | odbc | 0.06 | 10,000 | | **10000/10000**, 10000/10000 | BIG → VARCHAR; DF: VARCHAR → **DOUBLE**; TS_TZ, T_TZ: time zone **dropped** |
| `current_timestamp_type` | query | 0.00 | 1 | 1 | - | - |
| | odbc | 0.02 | 1 | | n/a (time of run) | TS: TIMESTAMP WITH TIME ZONE → TIMESTAMP |

## What it shows

- **A join filtered through its header table.** Written in DuckDB over ATTACHed tables, the date filter reaches only `PAR_ORDER`, so Firebird sends all 1 M detail rows to keep 90,680. That's 12× the rows sent and 3.6× the time on the same host, and the gap grows with network latency. `firebird_query` lets the server use its indexes and sends only the result.
- **An aggregate.** On the same host DuckDB aggregates 1.25 M shipped rows as fast as Firebird does on the server. `firebird_query` sends 180 rows instead of 1.25 M, which matters as soon as the server is remote.
- **What only `firebird_query` can reach:** a selectable stored procedure. ATTACH can't call it. Time equals ODBC's.
- **Values are the same as ATTACH everywhere.** `firebird_query` uses the extension's type mapping, so it inherits the existing fidelity: trimmed CHAR, BLOB bytes past a NUL, INT128 as `DECIMAL(38,x)`, exact `DECFLOAT`, and time zones.
- **The usual ODBC path loses data in four places:**
  - it truncates text BLOBs at an embedded NUL (50 rows);
  - it turns `NUMERIC(38,x)` into text;
  - it turns `DECFLOAT(34)` into DOUBLE, losing digits;
  - it drops time zones.

  Checked against the server with isql. For example, `DECFLOAT(34)` `1/3` arrives natively with all 34 digits; ODBC delivers a 16-digit DOUBLE.
- **Computed columns:** the same cost on every path; the cost is on the server.

A speedup seen on one production query heavy on selectable procedures (2.5× faster than ODBC) **doesn't reproduce here**: `selectable_procedure` takes the same time on both paths. So it isn't claimed.
