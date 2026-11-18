// firebird_execute(alias, sql): runs one statement that returns no rows on
// an ATTACH opened with (TYPE firebird, READ_WRITE), inside the DuckDB
// transaction's Firebird write transaction.
//
// The write transaction starts on the first write of the DuckDB transaction,
// on a connection of the ATTACH's write pool (never the read pool), and is
// committed or rolled back with the DuckDB transaction: one Firebird
// transaction per statement in autocommit, one per BEGIN ... COMMIT.
//
// Returns one row: rows_affected (inserted + updated + deleted, as Firebird
// reports them) and executions.

#include <cctype>

#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include "firebird_client.hpp"
#include "firebird_query_function.hpp"

namespace duckdb {

namespace {

constexpr const char *kFunctionName = "firebird_execute";

struct FirebirdExecuteBindData : public TableFunctionData {
    std::string alias;
    std::string sql;
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
    bind->attach = GetFirebirdAttachHandle(context, bind->alias, kFunctionName);
    CheckWritable(bind->attach, bind->alias);

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
    if (IsCreateDatabaseStatement(bind.sql)) {
        // Creates another database; this ATTACH's transaction is not involved.
        FirebirdCreateDatabase(bind.sql, bind.attach.dialect);
    } else {
        auto &conn = GetFirebirdWriteConnection(context, *bind.attach.catalog, bind.lock_timeout_s);
        FirebirdStatement stmt(conn, bind.sql, FirebirdStatement::PrepareOnlyTag {});
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
        stmt.ExecuteNoCursor(bind.sql);
        rows_affected = stmt.RecordsAffected();
    }

    output.SetCardinality(1);
    output.data[0].SetValue(0, Value::BIGINT(rows_affected));
    output.data[1].SetValue(0, Value::BIGINT(1));
}

} // namespace

TableFunction GetFirebirdExecuteFunction() {
    TableFunction fn(kFunctionName,
#ifdef FIREBIRD_DUCKDB_V2
                     FunctionSignature()
                         .AddPositionalOnly("alias", LogicalType::VARCHAR)
                         .AddPositionalOnly("sql", LogicalType::VARCHAR),
#else
                     {LogicalType::VARCHAR, LogicalType::VARCHAR},
#endif
                     FirebirdExecuteFunction, FirebirdExecuteBind, FirebirdExecuteInitGlobal);
    fn.projection_pushdown = false;
    return fn;
}

} // namespace duckdb
