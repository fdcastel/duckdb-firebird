// firebird_execute(alias, sql [, source := '<duckdb query>',
//                  commit_every := N, count_rows := true]):
// runs a statement that returns no rows on an ATTACH opened with
// (TYPE firebird, READ_WRITE), inside the DuckDB transaction's Firebird
// write transaction.
//
// The write transaction starts on the first write of the DuckDB transaction,
// on a connection of the ATTACH's write pool (never the read pool), and is
// committed or rolled back with the DuckDB transaction: one Firebird
// transaction per statement in autocommit, one per BEGIN ... COMMIT.
//
// With `source`, the statement is prepared once and executed once per row
// of the DuckDB query, with the row's columns as its `?` parameters (column
// i -> parameter i), encoded by FirebirdParamEncoder. commit_every = N
// commits every N executions (those rows stay committed if a later row
// fails); 0 (default) commits with the DuckDB transaction.
//
// Returns one row: rows_affected (inserted + updated + deleted, as Firebird
// reports them; NULL with count_rows := false) and executions.

#include <cctype>
#include <cctype>
#include <chrono>
#include <regex>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#ifdef FIREBIRD_DUCKDB_V2
#include "duckdb/main/query_result_stream.hpp"
#endif
#include "duckdb/planner/binder.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include "firebird_client.hpp"
#include "firebird_observability.hpp"
#include "firebird_param_encoder.hpp"
#include "firebird_query_function.hpp"

namespace duckdb {

namespace {

constexpr const char *kFunctionName = "firebird_execute";

struct FirebirdExecuteBindData : public TableFunctionData {
    std::string alias;
    std::string sql;
    // The SQL as the query log and error messages show it, and the
    // password literals it hides (see RedactPasswords).
    std::string logged_sql;
    std::vector<std::string> secrets;
    FirebirdAttachHandle attach;
    int64_t lock_timeout_s = 10;
    bool has_source = false;
    std::string source;
    vector<LogicalType> source_types;
    int64_t commit_every = 0;
    bool count_rows = true;
};

struct FirebirdExecuteGlobalState : public GlobalTableFunctionState {
    bool done = false;
};

void CheckWritable(const FirebirdAttachHandle &attach, const std::string &alias) {
    if (attach.read_only) {
        throw BinderException("%s: ATTACH '%s' is READ_ONLY", kFunctionName, alias);
    }
    if (!attach.write_enabled) {
        throw BinderException("%s: ATTACH '%s' does not allow writes. Attach it with "
                              "(TYPE firebird, READ_WRITE) to write to Firebird.",
                              kFunctionName, alias);
    }
}

// The first words of a statement, upper-cased, after leading spaces and
// comments (at most `count`).
std::vector<std::string> LeadingWords(const std::string &sql, size_t count) {
    std::vector<std::string> words;
    size_t i = 0;
    while (i < sql.size() && words.size() < count) {
        const unsigned char c = static_cast<unsigned char>(sql[i]);
        if (std::isspace(c)) {
            ++i;
        } else if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
            while (i < sql.size() && sql[i] != '\n') {
                ++i;
            }
        } else if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
            const auto end = sql.find("*/", i + 2);
            i = end == std::string::npos ? sql.size() : end + 2;
        } else if (std::isalpha(c)) {
            const size_t start = i;
            while (i < sql.size() && (std::isalnum(static_cast<unsigned char>(sql[i])) || sql[i] == '_')) {
                ++i;
            }
            words.push_back(StringUtil::Upper(sql.substr(start, i - start)));
        } else {
            break;
        }
    }
    return words;
}

// Session statements change the attachment, not the data. The write
// connection goes back to the pool when the DuckDB transaction ends and
// serves later writes of other DuckDB connections, which would inherit the
// change (a role, a time zone, an idle timeout that makes the server close
// it), so they are refused like transaction control.
void CheckSessionStatement(const std::string &sql) {
    const auto w = LeadingWords(sql, 3);
    std::string what;
    if (w.size() >= 2 && w[0] == "ALTER" && w[1] == "SESSION") {
        what = "ALTER SESSION";
    } else if (w.size() >= 2 && w[0] == "SET") {
        static const char *const kOneWord[] = {"ROLE", "BIND", "DECFLOAT", "SESSION", "OPTIMIZE", "SEARCH_PATH"};
        static const char *const kTwoWords[][2] = {{"TRUSTED", "ROLE"}, {"TIME", "ZONE"}, {"STATEMENT", "TIMEOUT"}};
        for (auto *word : kOneWord) {
            if (w[1] == word) {
                what = "SET " + w[1];
            }
        }
        for (auto &pair : kTwoWords) {
            if (w[1] == pair[0] && w.size() >= 3 && w[2] == pair[1]) {
                what = "SET " + w[1] + " " + w[2];
            }
        }
    }
    if (!what.empty()) {
        throw BinderException("%s: %s is refused: it changes the session of the write connection, which goes back "
                              "to the ATTACH's pool and would keep the change for later writes",
                              kFunctionName, what);
    }
}

// The SQL as the query log and error messages show it: every literal after
// PASSWORD (CREATE / ALTER USER, EXECUTE STATEMENT ... PASSWORD) becomes
// '***'. secrets gets each literal's text, as written and with '' undone.
std::string RedactPasswords(const std::string &sql, std::vector<std::string> &secrets) {
    static const std::regex kPassword(R"(\bPASSWORD(\s+)'((?:[^']|'')*)')",
                                      std::regex::ECMAScript | std::regex::icase);
    std::string out;
    auto last = sql.cbegin();
    for (std::sregex_iterator it(sql.cbegin(), sql.cend(), kPassword), end; it != end; ++it) {
        const auto &m = *it;
        out.append(last, m[0].first);
        out += m.str(0).substr(0, 8) + m.str(1) + "'***'";
        const std::string written = m.str(2);
        if (!written.empty()) {
            secrets.push_back(written);
            const std::string plain = StringUtil::Replace(written, "''", "'");
            if (plain != written) {
                secrets.push_back(plain);
            }
        }
        last = m[0].second;
    }
    out.append(last, sql.cend());
    return out;
}

// An error message with the SQL and its password literals hidden.
std::string RedactMessage(const std::string &message, const FirebirdExecuteBindData &bind) {
    std::string out = StringUtil::Replace(message, bind.sql, bind.logged_sql);
    for (auto &secret : bind.secrets) {
        out = StringUtil::Replace(out, secret, "***");
    }
    return out;
}

// Transaction control belongs to DuckDB. A statement that returns rows
// (SELECT, a selectable procedure, EXECUTE BLOCK with SUSPEND) is accepted
// only without a source: it runs, its rows are read and discarded (so a
// selectable procedure's side effects happen, as in isql), and
// rows_affected is NULL. Returns whether the statement returns rows.
bool CheckStatementType(FirebirdStatement &stmt, bool allow_rows) {
    const int type = stmt.StatementType();
    const bool returns_rows = type == isc_info_sql_stmt_select || type == isc_info_sql_stmt_select_for_upd;
    if (returns_rows && !allow_rows) {
        throw InvalidInputException("%s: with source, the statement must not return rows (this one does); "
                                    "use firebird_query() to read",
                                    kFunctionName);
    }
    if (type == isc_info_sql_stmt_start_trans || type == isc_info_sql_stmt_commit ||
        type == isc_info_sql_stmt_rollback) {
        throw InvalidInputException("%s: transactions follow the DuckDB transaction; use BEGIN, COMMIT "
                                    "and ROLLBACK in DuckDB instead of SET TRANSACTION, COMMIT or "
                                    "ROLLBACK in the statement",
                                    kFunctionName);
    }
    return returns_rows;
}

unique_ptr<FunctionData> FirebirdExecuteBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types,
#ifdef FIREBIRD_DUCKDB_V2
                                             vector<Identifier> &names) {
#else
                                             vector<string> &names) {
#endif
    if (input.inputs.size() != 2 || input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
        throw BinderException("%s(alias, sql) requires two non-NULL arguments", kFunctionName);
    }
    auto bind = make_uniq<FirebirdExecuteBindData>();
    bind->alias = input.inputs[0].ToString();
    bind->sql = input.inputs[1].ToString();
    bind->logged_sql = RedactPasswords(bind->sql, bind->secrets);
    for (auto &kv : input.named_parameters) {
#ifdef FIREBIRD_DUCKDB_V2
        if (kv.second.IsNull()) {
            continue;  // v2.0 fills omitted keyword-only parameters with NULL
        }
#endif
        if (kv.first == "source") {
            if (kv.second.IsNull()) {
                throw BinderException("%s: source must not be NULL", kFunctionName);
            }
            bind->has_source = true;
            bind->source = kv.second.ToString();
        } else if (kv.first == "commit_every") {
            bind->commit_every = kv.second.IsNull() ? 0 : kv.second.GetValue<int64_t>();
            if (bind->commit_every < 0) {
                throw BinderException("%s: commit_every must be >= 0 (0 = commit with the DuckDB "
                                      "transaction)",
                                      kFunctionName);
            }
        } else if (kv.first == "count_rows") {
            bind->count_rows = kv.second.IsNull() || kv.second.GetValue<bool>();
        }
    }
    bind->attach = GetFirebirdAttachHandle(context, bind->alias, kFunctionName);
    // Run the whole statement before the client sees its result, as DuckDB
    // does for INSERT. A streamed result commits only once it is fully read:
    // a client that read the one row with fetchone() and closed the
    // connection rolled every write back without an error. DuckDB 1.5
    // materializes the result with this output type; DuckDB 2.0 has none and
    // is handled in the function (see the autocommit commit there).
    // Registering the write marks the statement as a write on both.
    if (input.binder) {
        auto &properties = input.binder->GetStatementProperties();
#ifndef FIREBIRD_DUCKDB_V2
        properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
#endif
        if (!bind->attach.read_only && bind->attach.write_enabled) {
            properties.RegisterDBModify(*bind->attach.catalog, context,
                                        DatabaseModificationType(DatabaseModificationType::INSERT_DATA |
                                                                 DatabaseModificationType::UPDATE_DATA |
                                                                 DatabaseModificationType::DELETE_DATA));
        }
    }
    CheckWritable(bind->attach, bind->alias);
    CheckSessionStatement(bind->sql);
    if (bind->commit_every > 0 && !bind->has_source) {
        throw BinderException("%s: commit_every needs source := '<query>'", kFunctionName);
    }

    Value timeout;
    if (context.TryGetCurrentSetting("firebird_write_lock_timeout", timeout) && !timeout.IsNull()) {
        bind->lock_timeout_s = timeout.GetValue<int64_t>();
    }
    if (bind->lock_timeout_s < 0 || bind->lock_timeout_s > 32767) {
        throw BinderException("firebird_write_lock_timeout must be 0 (no wait) to 32767 seconds, got %lld",
                              static_cast<long long>(bind->lock_timeout_s));
    }

    if (bind->has_source) {
        // The source's columns, without running it.
        Connection con(*context.db);
        auto prepared = con.Prepare(bind->source);
        if (prepared->HasError()) {
            throw BinderException("%s: the source query does not prepare: %s", kFunctionName,
                                  prepared->GetError());
        }
        bind->source_types = prepared->GetTypes();
        // The statement's parameters, on a read connection (prepare only).
        idx_t param_count = 0;
        {
            auto conn = bind->attach.pool->Acquire();
            try {
                FirebirdStatement stmt(*conn, bind->sql, FirebirdStatement::PrepareOnlyTag {});
                CheckStatementType(stmt, false);
                param_count = stmt.DescribeInput().size();
            } catch (...) {
                bind->attach.pool->Release(std::move(conn));
                throw;
            }
            bind->attach.pool->Release(std::move(conn));
        }
        if (param_count != bind->source_types.size()) {
            throw BinderException("%s: the statement has %llu parameter(s) (?) but the source query "
                                  "returns %llu column(s); they are matched by position",
                                  kFunctionName, static_cast<unsigned long long>(param_count),
                                  static_cast<unsigned long long>(bind->source_types.size()));
        }
    }

    return_types = {LogicalType::BIGINT, LogicalType::BIGINT};
#ifdef FIREBIRD_DUCKDB_V2
    names = {Identifier("rows_affected"), Identifier("executions")};
#else
    names = {"rows_affected", "executions"};
#endif
    return std::move(bind);
}

unique_ptr<GlobalTableFunctionState> FirebirdExecuteInitGlobal(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<FirebirdExecuteGlobalState>();
}

void FirebirdExecuteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind = data.bind_data->Cast<FirebirdExecuteBindData>();
    auto &state = data.global_state->Cast<FirebirdExecuteGlobalState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    state.done = true;

    // commit_every commits the transaction's write connection, so inside
    // BEGIN ... COMMIT it would also commit the earlier writes of the DuckDB
    // transaction, which a ROLLBACK could then no longer undo.
    if (bind.commit_every > 0 && !context.transaction.IsAutoCommit()) {
        throw InvalidInputException("%s: commit_every needs autocommit; inside BEGIN ... COMMIT its commits would "
                                    "also commit the transaction's earlier writes",
                                    kFunctionName);
    }

    // A DuckDB transaction writes to one attached database at most; this
    // also makes a READ ONLY DuckDB transaction refuse the write.
    auto &db = bind.attach.catalog->GetAttached();
    MetaTransaction::Get(context).ModifyDatabase(
        db, DatabaseModificationType(DatabaseModificationType::INSERT_DATA | DatabaseModificationType::UPDATE_DATA |
                                     DatabaseModificationType::DELETE_DATA));

    int64_t rows_affected = 0;
    int64_t executions = 0;
    bool rows_unknown = !bind.count_rows;
    RecordFirebirdWrite(context, bind.logged_sql, "execute", "", -1);
    auto obs = GetObservabilityState(context);
    const auto t0 = std::chrono::steady_clock::now();
    try {
        // One write at a time on the DuckDB transaction's write connection:
        // two calls in one statement (UNION ALL, ...) can run in parallel
        // pipelines, and they wait for each other here. Held through the
        // commit below.
        auto writes = LockFirebirdWrites(context, *bind.attach.catalog);
        // Each branch frees its statement at its end, before the commit
        // below hands its connection back to the write pool (which may close it).
        if (!bind.has_source) {
            auto &conn = GetFirebirdWriteConnection(context, *bind.attach.catalog, bind.lock_timeout_s);
            FirebirdStatement stmt(conn, bind.sql, FirebirdStatement::PrepareOnlyTag {});
            const bool returns_rows = CheckStatementType(stmt, true);
            const bool is_ddl = stmt.StatementType() == isc_info_sql_stmt_ddl;
            if (is_ddl) {
                // Idle connections of this ATTACH would hold the tables they
                // used ("object ... is in use" on DROP / ALTER).
                ReleaseFirebirdIdleConnections(*bind.attach.catalog);
            }
            const auto params = stmt.DescribeInput().size();
            if (params > 0) {
                throw InvalidInputException("%s: the statement has %llu parameter(s) (?); pass their values with "
                                            "source := '<query>'",
                                            kFunctionName, static_cast<unsigned long long>(params));
            }
            if (returns_rows) {
                stmt.ExecuteCursor(bind.sql);
                while (stmt.Fetch()) {
                }
                rows_unknown = true;
            } else {
                stmt.ExecuteNoCursor(bind.sql);
                if (bind.count_rows) {
                    rows_affected = stmt.RecordsAffected();
                }
                if (is_ddl) {
                    // The ATTACH reloads its tables once this commits.
                    NoteFirebirdDdl(context, *bind.attach.catalog);
                }
            }
            executions = 1;
        } else {
            auto &conn = GetFirebirdWriteConnection(context, *bind.attach.catalog, bind.lock_timeout_s);
            FirebirdStatement stmt(conn, bind.sql, FirebirdStatement::PrepareOnlyTag {});
            CheckStatementType(stmt, false);
            FirebirdParamEncoder encoder(stmt, bind.source_types, bind.attach.none_encoding);

            // The source is read as a stream, a chunk at a time: its connection
            // buffers a chunk or two ahead of the writes (DuckDB's default read-ahead is
            // about 1 MB on 1.5 and 10 MB on 2.0, often 100,000 rows). Writing to
            // Firebird is the slow side, so a longer read-ahead gains nothing; it
            // would only hold more memory, and a source that fails within it
            // would fail before the rows ahead of the failure are written.
            Connection con(*context.db);
#ifdef FIREBIRD_DUCKDB_V2
            auto read_ahead = con.Query("SET max_streaming_buffer_size = '1KB'");
#else
            auto read_ahead = con.Query("SET streaming_buffer_size = '1KB'");
#endif
            if (read_ahead->HasError()) {
                throw InternalException("%s: could not limit the source's read-ahead: %s", kFunctionName,
                                        read_ahead->GetError());
            }
#ifdef FIREBIRD_DUCKDB_V2
            auto submitted = con.Submit(bind.source);
            if (submitted->HasError()) {
                throw InvalidInputException("%s: the source query failed: %s", kFunctionName, submitted->GetError());
            }
            QueryResultStream<> source(std::move(submitted));
#else
            auto result = con.SendQuery(bind.source);
            if (result->HasError()) {
                throw InvalidInputException("%s: the source query failed: %s", kFunctionName, result->GetError());
            }
            auto &source = *result;
#endif
            int64_t committed = 0;
            auto kept = [&]() -> std::string {
                return committed > 0 ? " Source rows 1 to " + std::to_string(committed) +
                                           " were committed (commit_every); the later ones are rolled back."
                                     : " Nothing of this call is kept: the transaction is rolled back.";
            };
            auto fail = [&](int64_t row, const std::string &message) {
                throw InvalidInputException("%s: source row %lld failed: %s%s", kFunctionName,
                                            static_cast<long long>(row), message, kept());
            };
            bool types_checked = false;
            while (true) {
                auto chunk = source.Fetch();
                if (!chunk || chunk->size() == 0) {
                    break;
                }
                if (!types_checked) {
                    // The encoder reads each value as the type found at bind.
                    // DuckDB doesn't bind a prepared statement again when a
                    // table the source reads changes (it isn't a dependency
                    // of this statement), so a column now of another type
                    // would be misread (5 read as 0.05).
                    const auto &now = chunk->GetTypes();
                    for (idx_t c = 0; c < now.size() && c < bind.source_types.size(); ++c) {
                        if (now[c] != bind.source_types[c]) {
                            throw InvalidInputException(
                                "%s: source column %llu was %s when the call was bound and is %s now; "
                                "prepare the statement again.%s",
                                kFunctionName, static_cast<unsigned long long>(c + 1),
                                bind.source_types[c].ToString(), now[c].ToString(), kept());
                        }
                    }
                    types_checked = true;
                }
                for (idx_t r = 0; r < chunk->size(); ++r) {
                    try {
                        encoder.Encode(*chunk, r);
                        stmt.ExecuteWithInput(bind.sql);
                        if (bind.count_rows) {
                            rows_affected += stmt.RecordsAffected();
                        }
                    } catch (std::exception &ex) {
                        fail(executions + 1, ErrorData(ex).RawMessage());
                    }
                    ++executions;
                    if (bind.commit_every > 0 && executions % bind.commit_every == 0) {
                        // A failed commit keeps only what earlier boundaries
                        // committed; once it succeeds, these rows are kept too,
                        // even if the next write transaction fails to start.
                        try {
                            conn.CommitWrite();
                        } catch (std::exception &ex) {
                            throw InvalidInputException("%s: the commit after source row %lld failed: %s%s",
                                                        kFunctionName, static_cast<long long>(executions),
                                                        ErrorData(ex).RawMessage(), kept());
                        }
                        committed = executions;
                        try {
                            conn.StartWriteTransaction(bind.lock_timeout_s);
                        } catch (std::exception &ex) {
                            throw InvalidInputException("%s: the write transaction after source row %lld "
                                                        "could not start: %s%s",
                                                        kFunctionName, static_cast<long long>(executions),
                                                        ErrorData(ex).RawMessage(), kept());
                        }
                    }
                }
            }
            if (source.HasError()) {
                throw InvalidInputException("%s: the source query failed after %lld row(s): %s%s", kFunctionName,
                                            static_cast<long long>(executions), source.GetError(), kept());
            }
        }
#ifdef FIREBIRD_DUCKDB_V2
        // DuckDB 2.0 runs this statement's result lazily and rolls the statement
        // back when its client closes before reading it all (fetchone(), then
        // close): the write would be lost without an error. In autocommit the
        // statement is the transaction, so commit the Firebird write here,
        // before the row is returned. Inside BEGIN ... COMMIT nothing changes.
        // (1.5 materializes the result instead; see the bind.)
        if (context.transaction.IsAutoCommit()) {
            CommitFirebirdWriteNow(context, *bind.attach.catalog);
        }
#endif
    } catch (std::exception &ex) {
        ErrorData error(ex);
        if (!bind.secrets.empty()) {
            error = ErrorData(error.Type(), RedactMessage(error.RawMessage(), bind));
        }
        obs->SetWriteResult("row", executions, -1);
        obs->SetError(error.RawMessage());
        if (!bind.secrets.empty()) {
            error.Throw();
        }
        throw;
    }
    obs->SetWriteResult("row", executions, rows_unknown ? -1 : rows_affected);
    obs->AddFirebirdTimeUs(
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0).count());
    obs->UpdateTotalTimeUs();

    output.SetCardinality(1);
    output.data[0].SetValue(0, rows_unknown ? Value(LogicalType::BIGINT) : Value::BIGINT(rows_affected));
    output.data[1].SetValue(0, Value::BIGINT(executions));
}

// --- firebird_clear_cache(alias) ----------------------------------------------

struct FirebirdClearCacheBindData : public TableFunctionData {
    FirebirdAttachHandle attach;
};

struct FirebirdClearCacheState : public GlobalTableFunctionState {
    bool done = false;
};

unique_ptr<FunctionData> FirebirdClearCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types,
#ifdef FIREBIRD_DUCKDB_V2
                                                vector<Identifier> &names) {
#else
                                                vector<string> &names) {
#endif
    if (input.inputs.size() != 1 || input.inputs[0].IsNull()) {
        throw BinderException("firebird_clear_cache(alias) requires a non-NULL alias");
    }
    auto bind = make_uniq<FirebirdClearCacheBindData>();
    bind->attach = GetFirebirdAttachHandle(context, input.inputs[0].ToString(), "firebird_clear_cache");
    return_types = {LogicalType::BOOLEAN};
#ifdef FIREBIRD_DUCKDB_V2
    names = {Identifier("cleared")};
#else
    names = {"cleared"};
#endif
    return std::move(bind);
}

unique_ptr<GlobalTableFunctionState> FirebirdClearCacheInit(ClientContext &, TableFunctionInitInput &) {
    return make_uniq<FirebirdClearCacheState>();
}

void FirebirdClearCacheFunction(ClientContext &, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.global_state->Cast<FirebirdClearCacheState>();
    if (state.done) {
        output.SetCardinality(0);
        return;
    }
    state.done = true;
    ClearFirebirdCatalogCache(*data.bind_data->Cast<FirebirdClearCacheBindData>().attach.catalog);
    output.SetCardinality(1);
    output.data[0].SetValue(0, Value::BOOLEAN(true));
}

} // namespace

TableFunction GetFirebirdClearCacheFunction() {
    return TableFunction("firebird_clear_cache",
#ifdef FIREBIRD_DUCKDB_V2
                         FunctionSignature().AddPositionalOnly("alias", LogicalType::VARCHAR),
#else
                         {LogicalType::VARCHAR},
#endif
                         FirebirdClearCacheFunction, FirebirdClearCacheBind, FirebirdClearCacheInit);
}

TableFunction GetFirebirdExecuteFunction() {
#ifdef FIREBIRD_DUCKDB_V2
    TableFunction fn(kFunctionName,
                     FunctionSignature()
                         .AddPositionalOnly("alias", LogicalType::VARCHAR)
                         .AddPositionalOnly("sql", LogicalType::VARCHAR)
                         .AddKeywordOnly("source", LogicalType::VARCHAR, Value(LogicalType::VARCHAR))
                         .AddKeywordOnly("commit_every", LogicalType::BIGINT, Value(LogicalType::BIGINT))
                         .AddKeywordOnly("count_rows", LogicalType::BOOLEAN, Value(LogicalType::BOOLEAN)),
                     FirebirdExecuteFunction, FirebirdExecuteBind, FirebirdExecuteInitGlobal);
#else
    TableFunction fn(kFunctionName, {LogicalType::VARCHAR, LogicalType::VARCHAR}, FirebirdExecuteFunction,
                     FirebirdExecuteBind, FirebirdExecuteInitGlobal);
    fn.named_parameters["source"] = LogicalType::VARCHAR;
    fn.named_parameters["commit_every"] = LogicalType::BIGINT;
    fn.named_parameters["count_rows"] = LogicalType::BOOLEAN;
#endif
    fn.projection_pushdown = false;
    return fn;
}

} // namespace duckdb
