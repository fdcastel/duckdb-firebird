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

Setup: DuckDB 1.5.6, extension a400738 (branch `read-write`), official Firebird ODBC driver 3.0.1.21, best of 3. The standalone `firebird-query` build (a568181) gives the same values, rows and types, with times within 10%.

On another machine, the same queries on Firebird 3.0.13 and 4.0.6 (no ODBC driver there) gave 0/0 value differences on every query. Their timings are under "What it shows".

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

- **Rows on the wire, not raw speed.**
  - A join filtered through its header table: written in DuckDB over ATTACHed tables, the date filter reaches only `PAR_ORDER`, so Firebird sends all 1 M detail rows to keep 90,680. `firebird_query` sends only the result: **90,680 rows instead of 1,087,769**.
  - An aggregate: **180 rows instead of 1,250,000**.
  - Which path is faster depends on the machine. On the same host the extra rows cost little, DuckDB joins and aggregates them in parallel, and Firebird runs the query on one thread:
    - here (Firebird 5.0.4) the join takes 0.74 s with `firebird_query` against 2.65 s through ATTACH, and the aggregate ties;
    - on a second machine (Firebird 3.0.13 / 4.0.6) ATTACH won both, 1.56 s against 2.60 s and 1.16 s against 3.24 s on Firebird 3.
  - Over a real network the rows sent dominate. So `firebird_query` is presented for what ATTACH can't do and for the rows it doesn't move, not as faster in general.
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

## Writes: `parity_writes.py` (needs the read-write prototype build)

`parity_writes.py` writes the same rows three ways and reads every stored value back with `firebird_query`:
- `odbc_copy`;
- `firebird_execute(alias, dml, source := …)`;
- `INSERT INTO fb.main.T SELECT`.

```sh
uv run --no-project --with duckdb==1.5.6 python scripts/parity/parity_writes.py \
    --database localhost:/data/parity.fdb --extension <read-write build> --odbc --rows 1000000
```

Results: Firebird 5.0.4, Windows, server on the same host, extension a400738, official ODBC driver 3.0.1.21.

| case | path | seconds | rows/s | missing | not sent | result |
|---|---|---:|---:|---:|---:|---|
| `sticky_null` (2,000 rows) | odbc_copy | 0.23 | 8,593 | **999** | **999** | **values differ** |
| | firebird_execute | 0.12 | 16,978 | 0 | 0 | exact |
| | insert_into | 0.15 | 13,119 | 0 | 0 | exact |
| `types` (3 rows, every family) | odbc_copy | 0.02 | – | **1** | **1** | **values differ** |
| | firebird_execute | 0.01 | – | 0 | 0 | exact |
| | insert_into | 0.04 | – | 0 | 0 | exact |
| `bulk` (1,000,000 rows) | odbc_copy | 112.63 | 8,879 | 0 | 0 | exact |
| | firebird_execute | **58.17** | **17,191** | 0 | 0 | exact |
| | insert_into | **58.41** | **17,120** | 0 | 0 | exact |
| `floor` (1,000,000 no-op `EXECUTE BLOCK`) | odbc_copy | 95.36 | 10,486 | | | 95.4 µs/row |
| | firebird_execute | 56.41 | 17,728 | | | 56.4 µs/row |

- **`sticky_null`:** the official ODBC driver stores NULL for every value that follows a NULL on a parameter whose source type differs from the column type (firebird-odbc-driver issue 300). Here that's 999 of the 1,000 non-NULL rows. Both native paths store every value.
- **`types`:** ODBC stores `TIME '23:59:59.9999'` as `23:59:59`, dropping the fractional seconds. Every value round-trips natively: 150 KB text BLOB, pre-1900 dates, `NUMERIC(18,4)` extremes, UTF-8 text, BOOLEAN.
- **`floor`:** a statement that does nothing costs 56 µs per row natively and 95 µs through ODBC. On the same host, a simple INSERT takes 58 µs per row, so nearly all of it is per-row round-trip overhead. That's the case for batching.
- **DDL:** `firebird_execute` refreshes the ATTACH's tables when DDL commits, so `INSERT INTO fb.main.T` sees a table the script just recreated, with no re-attach.

### Batched writes (`--batch-sizes`, Firebird 4+)

`SET firebird_write_batch_size = N` sends N rows per Firebird `IBatch` round trip instead of one.
- Firebird 5 batches every statement, `EXECUTE BLOCK` included.
- Firebird 4 batches plain INSERT/UPDATE/DELETE only. It rejects a batched `EXECUTE BLOCK` with "request synchronization error".
- Firebird 3, and statements with BLOB parameters, stay per row.

`upsert` is an `EXECUTE BLOCK` `UPDATE OR INSERT ... MATCHING (ID)` into a table with a primary key, with the same rows as `bulk`.

Firebird 5.0.4, Windows, server on the same host, 1,000,000 rows, extension fb73b5f. Every row reads back exact at every batch size; `sticky_null` and `types` too.

| case | ODBC | per row | batch 100 | batch 1000 | batch 10000 |
|---|---:|---:|---:|---:|---:|
| `bulk`, `firebird_execute` | 121.5 s | 68.0 s | 4.4 s | 2.9 s | **2.7 s** |
| `bulk`, `INSERT INTO` | | 61.7 s | 3.9 s | 2.9 s | **2.6 s** |
| `upsert`, `firebird_execute` | 131.9 s | 69.5 s | 8.8 s | 7.8 s | **7.5 s** |
| `floor` | 100.8 µs/row | 54.0 µs/row | 2.4 µs/row | 1.3 µs/row | 1.4 µs/row |

- **The gain:** with no network at all, batching takes the insert 25× and the upsert 9× past per-row native. Most of it arrives by batch 1000.
- **Over a network:** with 1 ms of round trip, 100,000 rows took 91.5 s → 0.36 s for `bulk` and 89.8 s → 0.70 s for `upsert`, at batch 10000.
- **Where it stops:** a statement with heavy server work gains less. A 25-parameter `EXECUTE BLOCK` upsert with lookups, into a large real-world table, went from 24 s per row to 11 s at batch 1000 (ODBC: 32.5 s).
- **Wide rows:** the batch buffer is 16 MB, and a batch is sent before it fills. So `firebird_write_batch_size` is an upper bound for wide rows, never an error.