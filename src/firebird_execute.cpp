// firebird_execute(alias, sql): runs one statement on an ATTACH opened with
// (TYPE firebird, READ_WRITE), inside the DuckDB transaction's Firebird
// write transaction.
//
// The write transaction starts on the first write of the DuckDB transaction,
// on a connection of the ATTACH's write pool (never the read pool), and is
// committed or rolled back with the DuckDB transaction: one Firebird
// transaction per statement in autocommit, one per BEGIN ... COMMIT.
//
// Returns one row: rows_affected (inserted + updated + deleted, as Firebird
// reports them; NULL for a statement that returns rows) and executions.

#include <cctype>
#include <chrono>
#include <regex>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include "firebird_client.hpp"
#include "firebird_observability.hpp"
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
// (SELECT, a selectable procedure, EXECUTE BLOCK with SUSPEND) is accepted:
// it runs, its rows are read and discarded (so a selectable procedure's side
// effects happen, as in isql), and rows_affected is NULL. Returns whether
// the statement returns rows.
bool CheckStatementType(FirebirdStatement &stmt) {
    const int type = stmt.StatementType();
    if (type == isc_info_sql_stmt_start_trans || type == isc_info_sql_stmt_commit ||
        type == isc_info_sql_stmt_rollback) {
        throw InvalidInputException("%s: transactions follow the DuckDB transaction; use BEGIN, COMMIT "
                                    "and ROLLBACK in DuckDB instead of SET TRANSACTION, COMMIT or "
                                    "ROLLBACK in the statement",
                                    kFunctionName);
    }
    return type == isc_info_sql_stmt_select || type == isc_info_sql_stmt_select_for_upd;
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

    Value timeout;
    if (context.TryGetCurrentSetting("firebird_write_lock_timeout", timeout) && !timeout.IsNull()) {
        bind->lock_timeout_s = timeout.GetValue<int64_t>();
    }
    if (bind->lock_timeout_s < 0 || bind->lock_timeout_s > 32767) {
        throw BinderException("firebird_write_lock_timeout must be 0 (no wait) to 32767 seconds, got %lld",
                              static_cast<long long>(bind->lock_timeout_s));
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

    // A DuckDB transaction writes to one attached database at most; this
    // also makes a READ ONLY DuckDB transaction refuse the write.
    auto &db = bind.attach.catalog->GetAttached();
    MetaTransaction::Get(context).ModifyDatabase(
        db, DatabaseModificationType(DatabaseModificationType::INSERT_DATA | DatabaseModificationType::UPDATE_DATA |
                                     DatabaseModificationType::DELETE_DATA));

    int64_t rows_affected = 0;
    int64_t executions = 0;
    bool rows_unknown = false;
    RecordFirebirdWrite(context, bind.logged_sql, "execute", "", -1);
    auto obs = GetObservabilityState(context);
    const auto t0 = std::chrono::steady_clock::now();
    try {
        // One write at a time on the DuckDB transaction's write connection:
        // two calls in one statement (UNION ALL, ...) can run in parallel
        // pipelines, and they wait for each other here. Held through the
        // commit below.
        auto writes = LockFirebirdWrites(context, *bind.attach.catalog);
        // The statement is freed at the end of this block, before the commit
        // below hands its connection back to the write pool (which may close it).
        {
            auto &conn = GetFirebirdWriteConnection(context, *bind.attach.catalog, bind.lock_timeout_s);
            FirebirdStatement stmt(conn, bind.sql, FirebirdStatement::PrepareOnlyTag {});
            const bool returns_rows = CheckStatementType(stmt);
            const bool is_ddl = stmt.StatementType() == isc_info_sql_stmt_ddl;
            if (is_ddl) {
                // Idle connections of this ATTACH would hold the tables they
                // used ("object ... is in use" on DROP / ALTER).
                ReleaseFirebirdIdleConnections(*bind.attach.catalog);
            }
            const auto params = stmt.DescribeInput().size();
            if (params > 0) {
                throw InvalidInputException("%s: the statement has %llu parameter(s) (?); firebird_execute "
                                            "takes no parameter values",
                                            kFunctionName, static_cast<unsigned long long>(params));
            }
            if (returns_rows) {
                stmt.ExecuteCursor(bind.sql);
                while (stmt.Fetch()) {
                }
                rows_unknown = true;
            } else {
                stmt.ExecuteNoCursor(bind.sql);
                rows_affected = stmt.RecordsAffected();
                if (is_ddl) {
                    // The ATTACH reloads its tables once this commits.
                    NoteFirebirdDdl(context, *bind.attach.catalog);
                }
            }
            executions = 1;
        }
#ifdef FIREBIRD_DUCKDB_V2
        // DuckDB 2.0 runs this statement's result lazily and rolls the
        // statement back when its client closes before reading it all
        // (fetchone(), then close): the write would be lost without an
        // error. In autocommit the statement is the transaction, so commit
        // the Firebird write here, before the row is returned. Inside
        // BEGIN ... COMMIT nothing changes. (1.5 materializes the result
        // instead; see the bind.)
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
                         .AddPositionalOnly("sql", LogicalType::VARCHAR),
                     FirebirdExecuteFunction, FirebirdExecuteBind, FirebirdExecuteInitGlobal);
#else
    TableFunction fn(kFunctionName, {LogicalType::VARCHAR, LogicalType::VARCHAR}, FirebirdExecuteFunction,
                     FirebirdExecuteBind, FirebirdExecuteInitGlobal);
#endif
    fn.projection_pushdown = false;
    return fn;
}

} // namespace duckdb
