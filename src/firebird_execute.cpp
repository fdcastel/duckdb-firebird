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

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include "firebird_client.hpp"
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

// SELECTs belong to firebird_query; transaction control to DuckDB.
void CheckStatementType(FirebirdStatement &stmt) {
    const int type = stmt.StatementType();
    if (type == isc_info_sql_stmt_select || type == isc_info_sql_stmt_select_for_upd) {
        throw InvalidInputException("%s: this statement returns rows; use firebird_query() to read",
                                    kFunctionName);
    }
    if (type == isc_info_sql_stmt_start_trans || type == isc_info_sql_stmt_commit ||
        type == isc_info_sql_stmt_rollback) {
        throw InvalidInputException("%s: transactions follow the DuckDB transaction; use BEGIN, COMMIT "
                                    "and ROLLBACK in DuckDB instead of SET TRANSACTION, COMMIT or "
                                    "ROLLBACK in the statement",
                                    kFunctionName);
    }
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
                CheckStatementType(stmt);
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
    if (!bind.has_source && IsCreateDatabaseStatement(bind.sql)) {
        // Creates another database; this ATTACH's transaction is not involved.
        FirebirdCreateDatabase(bind.sql, bind.attach.dialect);
        executions = 1;
    } else if (!bind.has_source) {
        auto &conn = GetFirebirdWriteConnection(context, *bind.attach.catalog, bind.lock_timeout_s);
        FirebirdStatement stmt(conn, bind.sql, FirebirdStatement::PrepareOnlyTag {});
        CheckStatementType(stmt);
        const auto params = stmt.DescribeInput().size();
        if (params > 0) {
            throw InvalidInputException("%s: the statement has %llu parameter(s) (?); pass their values with "
                                        "source := '<query>'",
                                        kFunctionName, static_cast<unsigned long long>(params));
        }
        stmt.ExecuteNoCursor(bind.sql);
        if (bind.count_rows) {
            rows_affected = stmt.RecordsAffected();
        }
        executions = 1;
    } else {
        auto &conn = GetFirebirdWriteConnection(context, *bind.attach.catalog, bind.lock_timeout_s);
        FirebirdStatement stmt(conn, bind.sql, FirebirdStatement::PrepareOnlyTag {});
        CheckStatementType(stmt);
        FirebirdParamEncoder encoder(stmt, bind.source_types, bind.attach.none_encoding);

        Connection con(*context.db);
#ifdef FIREBIRD_DUCKDB_V2
        auto result = con.Query(bind.source);  // v2.0: no SendQuery (built, not tested)
#else
        auto result = con.SendQuery(bind.source);  // streamed
#endif
        if (result->HasError()) {
            throw InvalidInputException("%s: the source query failed: %s", kFunctionName, result->GetError());
        }
        int64_t committed = 0;
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
                    const std::string kept =
                        committed > 0 ? " Source rows 1 to " + std::to_string(committed) +
                                            " were committed (commit_every); the later ones are rolled back."
                                      : " Nothing of this call is kept: the transaction is rolled back.";
                    throw InvalidInputException("%s: source row %lld failed: %s%s", kFunctionName,
                                                static_cast<long long>(executions + 1), ErrorData(ex).RawMessage(),
                                                kept);
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

    output.SetCardinality(1);
    output.data[0].SetValue(0, bind.count_rows ? Value::BIGINT(rows_affected) : Value(LogicalType::BIGINT));
    output.data[1].SetValue(0, Value::BIGINT(executions));
}

} // namespace

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
