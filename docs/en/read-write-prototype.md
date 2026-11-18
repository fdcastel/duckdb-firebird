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
- **Statements:** anything that returns no rows: DML, DDL, `EXECUTE
  PROCEDURE`, EXECUTE BLOCK without SUSPEND. SELECTs are refused (use
  `firebird_query`), and so are COMMIT, ROLLBACK and SET TRANSACTION:
  transactions follow DuckDB's. `rows_affected` is what Firebird reports
  through `isc_info_sql_records` (inserted + updated + deleted, including
  the statements inside an EXECUTE BLOCK); `executions` is 1.
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
- SELECT and COMMIT are refused;
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
