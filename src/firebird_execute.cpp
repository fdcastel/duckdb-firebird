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
#include <chrono>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
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

    // A DuckDB transaction writes to one attached database at most; this
    // also makes a READ ONLY DuckDB transaction refuse the write.
    auto &db = bind.attach.catalog->GetAttached();
    MetaTransaction::Get(context).ModifyDatabase(
        db, DatabaseModificationType(DatabaseModificationType::INSERT_DATA | DatabaseModificationType::UPDATE_DATA |
                                     DatabaseModificationType::DELETE_DATA));

    int64_t rows_affected = 0;
    int64_t executions = 0;
    bool rows_unknown = !bind.count_rows;
    RecordFirebirdWrite(context, bind.sql, "execute", "", -1);
    auto obs = GetObservabilityState(context);
    const auto t0 = std::chrono::steady_clock::now();
    try {
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

        Connection con(*context.db);
#ifdef FIREBIRD_DUCKDB_V2
        // DuckDB 2.0: the source runs to completion first and its rows are
        // held in memory (its streaming API is not used here yet).
        auto result = con.Query(bind.source);
#else
        auto result = con.SendQuery(bind.source);  // streamed
#endif
        if (result->HasError()) {
            throw InvalidInputException("%s: the source query failed: %s", kFunctionName, result->GetError());
        }
        int64_t committed = 0;
        auto fail = [&](int64_t row, const std::string &message) {
            const std::string kept =
                committed > 0 ? " Source rows 1 to " + std::to_string(committed) +
                                    " were committed (commit_every); the later ones are rolled back."
                              : " Nothing of this call is kept: the transaction is rolled back.";
            throw InvalidInputException("%s: source row %lld failed: %s%s", kFunctionName,
                                        static_cast<long long>(row), message, kept);
        };
        while (true) {
            auto chunk = result->Fetch();
            if (!chunk || chunk->size() == 0) {
                break;
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
                    conn.CommitWrite();
                    conn.StartWriteTransaction(bind.lock_timeout_s);
                    committed = executions;
                }
            }
        }
        if (result->HasError()) {
            throw InvalidInputException("%s: the source query failed after %lld row(s): %s", kFunctionName,
                                        static_cast<long long>(executions), result->GetError());
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
        obs->SetWriteResult("row", executions, -1);
        obs->SetError(ErrorData(ex).RawMessage());
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
