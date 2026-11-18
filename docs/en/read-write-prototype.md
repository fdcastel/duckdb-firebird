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
