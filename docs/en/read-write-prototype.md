# Read-write prototype

Working notes for the read-write prototype on the `read-write` branch: what
each work package does, the decisions taken, and the known gaps. The branch
starts from upstream main at `107bd8e`.

The goal is to find out whether an ETL can talk to Firebird through this
extension alone: read with native Firebird SQL, write with parameterized
DML (for example `EXECUTE BLOCK ... UPDATE OR INSERT`, one execution per
DuckDB row), with identical values and no loss of speed. Correct values on
the write path come first.

Test data is synthetic: `scripts/fixture_rw.sql` (Firebird 3+) and
`scripts/fixture_rw4.sql` (Firebird 4+ types), loaded into their own
database (`rw.fdb`; `FIREBIRD_RW_DB`, and `FIREBIRD_RW4_DB` on Firebird
4/5).

## WP1: `firebird_query(alias, sql)`

```sql
ATTACH 'user=SYSDBA;password=...;database=host:/path/db.fdb;charset=UTF8' AS fb (TYPE firebird);
SELECT * FROM firebird_query('fb', 'SELECT DEPT_NO, COUNT(*) FROM EMPLOYEE GROUP BY 1');
```

Runs one statement on the attached database exactly as written and returns
its rows.

- **Bind:** the statement is prepared and described
  (`isc_dsql_prepare`), and its type is read (`isc_info_sql_stmt_type`).
  Only SELECT statements are accepted: plain SELECTs, CTEs, selectable
  procedures and EXECUTE BLOCK with SUSPEND. Anything else (UPDATE,
  `EXECUTE PROCEDURE`, DDL) fails at bind and points to `firebird_execute()`.
- **Types:** each described column maps to the DuckDB type a scan gives
  the same Firebird type, with the ATTACH's `none_encoding` and
  `numeric_widen_int64`. CHARACTER SET NONE text (columns and text BLOBs)
  is decoded by `none_encoding`. DECFLOAT has no binary decoder here: the
  scan casts it to `VARCHAR(64)` on the server, and `firebird_query` asks
  for the same conversion through the output descriptor, so both return
  the same text. DataTypeCompatibility settings on the server don't change
  the result, because every attachment asks for the native types.
- **Execution:** one connection from the ATTACH's pool, in its read-only
  read-committed transaction, so a procedure that writes fails on the
  server ("attempted update during read-only transaction"). With
  `firebird_transaction_snapshot` on, the query reads the snapshot of the
  DuckDB transaction, like the scans do. The statement is prepared again at
  execution; if its columns changed since bind, the query fails and asks to
  run it again. There is no pushdown and no partitioning: DuckDB filters
  and projections over the result run in DuckDB.
- **Column names:** Firebird's names (aliases). Duplicates get DuckDB's
  usual suffixes, and an empty name becomes `columnN`.
- **Arguments:** both arguments may be any constant expression, for
  example `firebird_query('fb', getvariable('src_query'))`.
- **Telemetry:** one `firebird_last_query()` / `firebird_query_log()`
  record per call, with the SQL as sent, `rows_read`, the payload estimate
  and timings. `table_name` is empty, `partitions` is 1, and `snapshot_mode`
  is `serial` or `transaction`.

Tests: `test/sql/firebird_query.test` (Firebird 3/4/5) and
`test/sql/firebird_query_fb4.test` (INT128, NUMERIC(38,6), DECFLOAT, the
time zone types). Every type family is compared with a scan of the same
table both ways (`EXCEPT ALL`), and with the expected values. The other
cases: joins and aggregates, a recursive CTE, a selectable procedure,
Firebird-only syntax, an empty result, `getvariable()` arguments,
non-SELECTs rejected at bind, a writing procedure rejected by the server
with nothing written, and the telemetry record.

### Connection-string passwords in errors

A connection string in the space-separated form of the function examples
(`'database=... user=... password=...'`) was not parsed as key=value: the
whole string, password included, became the database name, and the attach
error echoed it. Now:

- whitespace-separated `key=value` pairs parse like the `;` form when every
  token is a known key;
- attach errors are redacted: `password=` values, `scheme://user:pass@`
  user info, and any literal occurrence of the password;
- the database path shown by `duckdb_databases()` is redacted the same
  way.

DuckDB itself quotes the start of the failing statement in a bind error
(`LINE 1: SELECT * FROM firebird_scan('...`). A connection string written
as a literal in the statement can therefore still show up there. Passing
it through a variable (`getvariable('cs')`) keeps it out.

Test: `test/sql/firebird_secrets.test` (needs only the client library: it
connects to port 1). On upstream main it fails with
`isc_attach_database(database=localhost/1:/nowhere/x.fdb user=SYSDBA password=Sekr3tPw): ...`.

### Gaps

- `firebird_query` takes an ATTACH alias only, not a connection string.
- No named parameters: no `?` placeholders, no per-call `none_encoding`.

## WP2: writable ATTACH and `firebird_execute(alias, sql)`

```sql
ATTACH 'user=SYSDBA;password=...;database=host:/path/db.fdb;charset=UTF8' AS fb (TYPE firebird, READ_WRITE);
SELECT * FROM firebird_execute('fb', 'UPDATE CUSTOMER SET ACTIVE = TRUE WHERE ID = 1');
-- rows_affected | executions
```

- **Opt-in (D1):** DuckDB's own `READ_WRITE` keyword (`READ_WRITE true`
  works too). DuckDB passes it to the extension as an explicit access
  mode, distinct from its default (automatic), so no custom option is
  needed. Without it every write fails at bind, naming the option. A
  `READ_ONLY` ATTACH refuses writes too.
- **Transaction (D3, D5):** the first write of a DuckDB transaction starts
  a Firebird read-write transaction (read committed, record versions,
  wait for locks up to `firebird_write_lock_timeout` seconds, default 10,
  `0` = no wait). DuckDB's commit commits it, and DuckDB's rollback (including
  the one after an error) rolls it back. So autocommit gives one Firebird
  transaction per DuckDB statement, and `BEGIN ... COMMIT` gives one
  Firebird transaction for all its writes. Explicit transactions cost
  nothing extra, so they were not refused (D5). A DuckDB transaction can
  write to one attached database only (DuckDB's rule, applied through
  `MetaTransaction::ModifyDatabase`).
- **Connections:** writes use a second pool of the ATTACH, so a
  connection that held a write transaction never serves a scan or
  `firebird_query`. A write connection goes back to its pool after commit
  or rollback, on a fresh read-only transaction. The write transaction is
  independent of the transaction snapshot coordinator (upstream issue 104
  work): reads, including reads in the same DuckDB transaction, do not see
  uncommitted writes.
- **Statements:** any one statement: DML, DDL, `EXECUTE PROCEDURE`,
  EXECUTE BLOCK. COMMIT, ROLLBACK and SET TRANSACTION are refused:
  transactions follow DuckDB's. `rows_affected` is what Firebird reports
  through `isc_info_sql_records` (inserted + updated + deleted, including
  the statements inside an EXECUTE BLOCK); `executions` is 1.
- **Statements that return rows** (SELECT, a selectable procedure,
  EXECUTE BLOCK with SUSPEND) run in the write transaction. Their rows
  are read and discarded, so a selectable procedure's side effects
  happen as in isql, and `rows_affected` is NULL (unknown). This matches
  an "execute any statement" contract (ODBC exec, `postgres_execute`); to
  read the rows, use `firebird_query`. With `source` (WP3) they are
  refused.
- **DDL:** allowed and committed with the transaction. The ATTACH's
  catalog is not refreshed: a table created this way does not appear
  under `fb.main` until the database is attached again (`firebird_query`
  can read it at once).
- **CREATE DATABASE:** a statement starting with `CREATE DATABASE` runs
  through `isc_dsql_execute_immediate` without an attachment (as isql
  does) and creates the database it names, with the user and password it
  names. It needs a READ_WRITE ATTACH like any write; that ATTACH's
  transaction is not involved. Its errors don't quote the statement,
  since the statement carries a password.

Tests: `test/sql/firebird_execute.test` (Firebird 3/4/5; two DuckDB
connections):
- the opt-in and READ_ONLY are enforced;
- an autocommit write is visible to another connection, and a
  `BEGIN ... COMMIT` write only after the COMMIT;
- ROLLBACK, and an error inside the transaction, leave nothing;
- a statement that fails half-way leaves none of its own changes;
- EXECUTE BLOCK (2 rows affected), EXECUTE PROCEDURE and DDL run;
- `SELECT 1 FROM RDB$DATABASE` returns NULL|1, a selectable procedure's INSERT is committed (seen by another connection), a SELECT with a source and COMMIT are refused (on the previous code the SELECT case failed with the old refusal);
- a row lock held by another connection gives "update conflicts with
  concurrent update" after the lock timeout, not a hang;
- the read pool is still read-only afterwards.

`test/sql/firebird_execute_create_db.test` covers CREATE DATABASE: the
database exists afterwards, the error doesn't quote the statement, and
the opt-in is required. It needs `FIREBIRD_RW_CREATE_PREFIX`, the server
and directory for new databases.

### Gaps

- No telemetry record for `firebird_execute`.
- Reads don't see the uncommitted writes of their own DuckDB transaction.

## WP3: parameter encoder and `firebird_execute(..., source := ...)`

```sql
CALL firebird_execute('fb',
    $$EXECUTE BLOCK (ID INTEGER = ?, NAME VARCHAR(40) = ?, CITY VARCHAR(30) = ?) AS
      BEGIN UPDATE OR INSERT INTO CUSTOMER (ID, NAME, CITY) VALUES (:ID, :NAME, :CITY) MATCHING (ID); END$$,
    source := 'SELECT id, name, city FROM staging_customers',
    commit_every := 10000);
-- rows_affected | executions
```

- **Source (D2):** a DuckDB query string, run on its own DuckDB
  connection and read as a stream. Column i of each row is parameter i
  (`?`, in text order). The column count must match the parameter count
  (checked at bind, by preparing both). Because the source runs on its own
  connection, it sees committed DuckDB data and tables of the database,
  but not the calling connection's TEMP tables or uncommitted changes.
- **Execution:** the statement is prepared and its parameters described
  once; then one `isc_dsql_execute` per source row, in the DuckDB
  transaction's write transaction. `commit_every = N` commits every N
  executions; 0 (default) commits with the DuckDB transaction.
  `count_rows := false` skips the per-row `isc_info_sql_records` round
  trip and returns `rows_affected` NULL.
- **Errors:** the first failing row stops the call. The message names the
  source row and what is kept: nothing (rolled back with the DuckDB
  transaction), or source rows 1 to K when `commit_every` committed them.
- **Encoder (`FirebirdParamEncoder`, D6):** each value travels in a
  Firebird type that represents it exactly, and the server converts it to
  the parameter's declared type with its own rules. Rounding, overflow
  and truncation are therefore exactly what the same literal in the SQL
  text gets.
  - Integers go as BIGINT. A DECIMAL(p,s) goes as a BIGINT with scale s,
    so 1.5 lands in NUMERIC(9,3) as 1.500. A 128-bit value goes as INT128
    when the parameter is INT128; otherwise as a scaled BIGINT when it
    fits (trailing zero decimals dropped), else as text. Firebird 3
    overflows converting text with more digits than its target holds.
  - FLOAT, DOUBLE, BOOLEAN, DATE, TIME and TIMESTAMP go in their own
    types. TIME/TIMESTAMP WITH TIME ZONE go as UTC with the GMT zone, or
    as the UTC wall-clock time when the parameter has no zone. Firebird
    keeps 1/10000 s, so digits below 100 µs are dropped.
  - VARCHAR, and anything without a native form (DECFLOAT targets, UUID,
    ...), goes as VARCHAR in the parameter's character set. A value longer
    than the parameter is an error, never silent truncation.
  - CHARACTER SET NONE targets get `none_encoding` bytes (WIN1252 or
    ISO-8859-1; STRICT and BLOB send UTF-8 as is). A character the
    encoding can't hold is an error.
  - Values for BLOB parameters become BLOBs created in the write
    transaction, in 32 KB segments, of any size.
  - Every execution resets every field of every parameter: type, length,
    data pointer and NULL indicator. A value after a NULL is never sent as
    NULL (the ODBC sticky-NULL defect).
- **`BindInputParameters`** (the scan's `?` filters) is replaced by the
  same encoder. Its old code ignored the parameter's scale: it copied the
  value rounded to an integer as the scaled integer, so 1.5 for a
  NUMERIC(9,3) parameter would have been sent as 2, meaning 0.002. It had
  no BLOB, INT128, DECFLOAT or time zone support either. The scan's
  pushdown only binds BOOLEAN, integer, FLOAT/DOUBLE, VARCHAR, DATE, TIME
  and TIMESTAMP values, never DECIMAL (`IsBindableType`), so scans were
  not affected in practice.

Tests: `test/sql/firebird_execute_source.test` (Firebird 3/4/5) reads
back stored values, not row counts:
- every Firebird 3 type family round-trips from `RW_TYPES` to `RW_TARGET`
  (both-way `EXCEPT ALL` = 0), and the NONE column stores WIN1252 bytes;
- sticky NULLs: NULL, value, NULL, value, NULL on NUMERIC(9,3), BIGINT,
  INTEGER, DOUBLE, VARCHAR, DATE, BOOLEAN and text BLOB;
- 1.5 into NUMERIC(9,3) from DECIMAL(2,1), DECIMAL(18,6), DECIMAL(38,10),
  DOUBLE, INTEGER and VARCHAR;
- more digits than the column store what the same literal stores;
- NUMERIC(18,4) limits and negative scaled values;
- overflow names the row and keeps nothing;
- 140 KB text and 80 KB binary BLOBs; an over-long VARCHAR fails, and so
  does a character WIN1252 can't hold;
- the upsert example verbatim, run twice (insert, then update);
- an error in row 4 keeps nothing, or rows 1 to 2 with `commit_every = 2`;
- a source reading the table it writes, with and without
  `commit_every = 1`: the source reads only committed rows (3 inserted
  each time, no feedback loop on this size; on Firebird 3 a long source
  cursor could see rows committed by `commit_every` while it reads);
- the binding checks and `count_rows := false`.

`test/sql/firebird_execute_source_fb4.test` covers INT128,
NUMERIC(38,6), DECFLOAT(16/34) and the time zone types, as a round trip
and from DuckDB types.

Negative controls (local builds, Firebird 5): with the NULL indicator not
reset after a NULL, the round trip fails (the row after the all-NULL row
differs). With the DECIMAL scale ignored, the first row fails with
"numeric value is out of range".

Cost of `rows_affected`: each count is one more round trip per row.
100,000 rows into a two-column table on a local Firebird 5 (loopback
TCP, one rough run) took 6.8 s with `count_rows := false` and 13.1 s with
the default count.

## WP4: `INSERT INTO fb.main.T ...`

```sql
ATTACH '...' AS fb (TYPE firebird, READ_WRITE);
INSERT INTO fb.main.CUSTOMER SELECT id, name, city FROM staging_customers;
INSERT INTO fb.main.CUSTOMER (NAME, ID) VALUES ('Fa', 6);
```

- `PlanInsert` builds a sink (`FIREBIRD_INSERT` in EXPLAIN) that prepares
  `INSERT INTO "T" ("C1", ...) VALUES (?, ...)` once and executes it per
  row with the WP3 encoder, in the WP2 write transaction. It returns the
  row count like any INSERT.
- **Columns:** DuckDB's binder casts the values to the ATTACH's column
  types before they reach the sink, so no extra cast step is needed.
  With a column list, only those columns are sent, and Firebird applies
  its defaults, triggers and generators to the others. Without one, every
  column is sent. A COMPUTED BY column then makes Firebird refuse the
  INSERT; name the columns.
- **Refused:** RETURNING and DEFAULT VALUES (both point to
  `firebird_execute`), and an ATTACH without READ_WRITE or with
  READ_ONLY. ON CONFLICT is refused by DuckDB's own binder before it gets
  here, because the ATTACH declares no unique keys. `DEFAULT` inside a
  VALUES list becomes NULL: DuckDB fills it from the column defaults it
  knows, and the ATTACH doesn't declare Firebird's.
- **DuckDB v2.0 (built, not tested):** v2.0 binds DEFAULT VALUES with
  every column mapped to a NULL value, so the refusal above doesn't
  fire. The INSERT then sends NULL for every column (seen in CI: Firebird
  refused the NULL primary key). The prototype tests run on DuckDB 1.5.6
  only.
- **Errors:** the first failing row stops the INSERT with "row N failed:
  <Firebird error>". The DuckDB transaction then rolls back, and with it
  every row of the INSERT.

Tests: `test/sql/firebird_insert.test` (Firebird 3/4/5):
- the opt-in and READ_ONLY are enforced;
- INSERT ... SELECT from a scan of another Firebird table, every type
  family exact (both-way `EXCEPT ALL` = 0);
- inserts from a DuckDB table and from VALUES;
- a column list leaves the other columns NULL; 1.5 lands in NUMERIC(9,3)
  as 1.500, and NONE text is stored as WIN1252;
- NULL into a NOT NULL column, and a duplicate key in row 2, give
  Firebird's error and keep nothing;
- RETURNING, DEFAULT VALUES and ON CONFLICT are refused;
- `BEGIN ... COMMIT` is visible to another connection only after the
  COMMIT, and ROLLBACK keeps nothing;
- 20,000 rows, checked by count and sum.

1,000,000 rows (`INSERT INTO fb.main.RW_LOG SELECT i, 'n' || i FROM
range(1000000)`) took 75.6 s on a local Firebird 5 over loopback TCP,
about 13,200 rows/s (one rough run, machine not otherwise measured).

### Gaps

- One round trip per row; no batching (the legacy API has none; IBatch is
  OO API only).
- The ATTACH's catalog is read-only metadata: CREATE TABLE, ALTER, DROP,
  UPDATE and DELETE through `fb.main` are not supported (WP7 sizes UPDATE
  and DELETE).

## WP7 (sizing only): `UPDATE` / `DELETE` through `fb.main`

Not implemented. This is an estimate of what it takes and what changes.

**What blocks it today.** DuckDB binds UPDATE and DELETE only on a scan
that names its table (`LogicalGet::GetTable()`, filled by the table
function's `get_bind_info`). `firebird_scan` has none, hence "Can only
delete from base table" / "Can only update base table". A throwaway spike
(not committed) registered `get_bind_info` returning the ATTACH's
`FirebirdTableEntry`. With it, `DELETE FROM fb.main.T WHERE ...` and
`UPDATE ...` bound and reached the catalog's `PlanDelete` / `PlanUpdate`
(which still throw). Ordinary SELECTs were unaffected: DuckDB then only
reads the table's name and statistics (none).

**What has to be built** (rough C++ line counts):

| Piece | Lines |
|---|---|
| `get_bind_info` + a table pointer in the bind data (spike-proven) | ~15 |
| `rowid` = `RDB$DB_KEY` (8 bytes = one `row_t`) for base tables: the query builder projects it instead of today's `CAST(NULL AS BIGINT)`, the fetch copies the 8 bytes | ~40 |
| Scans feeding an UPDATE/DELETE run on the DuckDB transaction's write connection (DB_KEY is only stable inside one transaction): one partition, no shared or transaction snapshot | ~80 |
| Materialize those scans before the first write (as postgres' `MaterializePostgresScans`), so a cursor never reads rows the same statement has changed | ~30 |
| `PlanUpdate` / `PlanDelete` sinks: `UPDATE "T" SET "C" = ?, ... WHERE RDB$DB_KEY = ?` / `DELETE ... WHERE RDB$DB_KEY = ?` per row, SET values through the WP3 encoder, DB_KEY as an 8-byte OCTETS parameter | ~200 |
| Encoder: a raw 8-byte OCTETS kind for the DB_KEY | ~20 |
| **Total** | **~400 C++ + ~250 lines of tests** |

**What changes or breaks:**
- `SELECT rowid FROM fb.main.T` returns the DB_KEY instead of NULL. For
  views (a DB_KEY of 8 bytes per base table) it stays NULL, and
  UPDATE/DELETE on a view is refused.
- A scan under UPDATE/DELETE gives up PK/DB_KEY partitioning and the
  transaction snapshot, and reads in the write transaction (read
  committed). It sees its own transaction's earlier writes, unlike every
  other scan.
- Each row is one round trip, like WP4. `DELETE FROM fb.main.T WHERE
  <pushable filter>` runs N statements, where `firebird_execute` would run
  one statement on the server. A faster design would send the whole
  UPDATE/DELETE when DuckDB's plan is a single filtered scan; that needs
  plan inspection beyond the catalog hooks.
- A row deleted or changed by another transaction between the scan and
  the write gives 0 rows affected for its DB_KEY. Detecting that costs the
  per-row records count (one more round trip), as `count_rows` does.
- RETURNING and MERGE INTO stay out of scope.

**Recommendation:** feasible in about 1 to 1.5 days with tests. It is
worth doing only if DWDb needs UPDATE/DELETE written as DuckDB SQL over
`fb.main`. Otherwise `firebird_execute` already covers both (one
server-side statement, or per-row with `source`), and faster.
