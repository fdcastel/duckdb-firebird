// INSERT INTO fb.main.T ... on an ATTACH opened with (TYPE firebird,
// READ_WRITE): a sink that prepares
//     INSERT INTO "T" ("C1", ...) VALUES (?, ...)
// once, with the columns of the INSERT's column list (all columns when it
// has none), and executes it once per row with FirebirdParamEncoder, in the
// DuckDB transaction's Firebird write transaction. Omitted columns are left
// to Firebird (defaults, triggers, generators). Returns the row count like
// any INSERT.

#include "firebird_insert.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

#include "firebird_client.hpp"
#include "firebird_param_encoder.hpp"
#include "firebird_query_function.hpp"
#include "firebird_types.hpp"

namespace duckdb {

namespace {

// v2.0 names catalog objects with Identifier; v1.5 with string.
#ifdef FIREBIRD_DUCKDB_V2
std::string Str(const Identifier &name) {
    return name.GetIdentifierName();
}
#endif
const std::string &Str(const std::string &name) {
    return name;
}

struct FirebirdInsertGlobalState : public GlobalSinkState {
    std::unique_ptr<FirebirdStatement> stmt;
    std::unique_ptr<FirebirdParamEncoder> encoder;
    int64_t rows = 0;
};

struct FirebirdInsertSourceState : public GlobalSourceState {
    bool finished = false;
};

class FirebirdInsert final : public PhysicalOperator {
public:
    FirebirdInsert(PhysicalPlan &physical_plan, vector<LogicalType> types, idx_t estimated_cardinality,
                   Catalog &catalog, std::string table_name, std::string sql, vector<LogicalType> value_types,
                   NoneEncoding none_encoding)
        : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
          catalog_(catalog), table_name_(std::move(table_name)), sql_(std::move(sql)),
          value_types_(std::move(value_types)), none_encoding_(none_encoding) {
    }

    string GetName() const override {
        return "FIREBIRD_INSERT";
    }

    InsertionOrderPreservingMap<string> ParamsToString() const override {
        InsertionOrderPreservingMap<string> result;
        result["Table"] = table_name_;
        return result;
    }

    // --- Sink: one execution per row ---------------------------------------
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
        int64_t lock_timeout_s = 10;
        Value v;
        if (context.TryGetCurrentSetting("firebird_write_lock_timeout", v) && !v.IsNull()) {
            lock_timeout_s = v.GetValue<int64_t>();
        }
        auto state = make_uniq<FirebirdInsertGlobalState>();
        auto &conn = GetFirebirdWriteConnection(context, catalog_, lock_timeout_s);
        state->stmt = make_uniq<FirebirdStatement>(conn, sql_, FirebirdStatement::PrepareOnlyTag {});
        state->encoder = make_uniq<FirebirdParamEncoder>(*state->stmt, value_types_, none_encoding_);
        return std::move(state);
    }

    SinkResultType Sink(ExecutionContext &, DataChunk &chunk, OperatorSinkInput &input) const override {
        auto &state = input.global_state.Cast<FirebirdInsertGlobalState>();
        for (idx_t r = 0; r < chunk.size(); ++r) {
            try {
                state.encoder->Encode(chunk, r);
                state.stmt->ExecuteWithInput(sql_);
            } catch (std::exception &ex) {
                throw InvalidInputException("INSERT into Firebird table \"%s\": row %lld failed: %s", table_name_,
                                            static_cast<long long>(state.rows + 1), ErrorData(ex).RawMessage());
            }
            ++state.rows;
        }
        return SinkResultType::NEED_MORE_INPUT;
    }

    bool IsSink() const override {
        return true;
    }

    bool ParallelSink() const override {
        return false;
    }

    bool SinkOrderDependent() const override {
        return true;
    }

    // --- Source: the row count ---------------------------------------------
    unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &) const override {
        return make_uniq<FirebirdInsertSourceState>();
    }

    SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk, OperatorSourceInput &input) const override {
        auto &source = input.global_state.Cast<FirebirdInsertSourceState>();
        if (source.finished) {
            return SourceResultType::FINISHED;
        }
        auto &state = sink_state->Cast<FirebirdInsertGlobalState>();
        chunk.SetCardinality(1);
        chunk.SetValue(0, 0, Value::BIGINT(state.rows));
        source.finished = true;
        return SourceResultType::FINISHED;
    }

    bool IsSource() const override {
        return true;
    }

private:
    Catalog &catalog_;
    std::string table_name_;
    std::string sql_;
    vector<LogicalType> value_types_;
    NoneEncoding none_encoding_;
};

} // namespace

PhysicalOperator &PlanFirebirdInsert(ClientContext &, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                     optional_ptr<PhysicalOperator> plan, Catalog &catalog, bool write_enabled,
                                     NoneEncoding none_encoding) {
    const auto &table = op.table;
    if (catalog.GetAttached().IsReadOnly()) {
        throw BinderException("INSERT into Firebird table \"%s\": ATTACH '%s' is READ_ONLY", Str(table.name),
                              Str(catalog.GetName()));
    }
    if (!write_enabled) {
        throw BinderException("INSERT into Firebird table \"%s\": ATTACH '%s' does not allow writes. Attach it "
                              "with (TYPE firebird, READ_WRITE) to write to Firebird.",
                              Str(table.name), Str(catalog.GetName()));
    }
    if (op.return_chunk) {
        throw BinderException("INSERT into Firebird table \"%s\": RETURNING is not supported", Str(table.name));
    }
    if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
        throw BinderException("INSERT into Firebird table \"%s\": ON CONFLICT is not supported; use "
                              "firebird_execute() with UPDATE OR INSERT ... MATCHING",
                              Str(table.name));
    }
    if (!plan) {
        throw BinderException("INSERT into Firebird table \"%s\": nothing to insert", Str(table.name));
    }

    // Column names in the order of the input columns.
    const auto &columns = table.GetColumns();
    vector<std::string> names;
    if (op.column_index_map.empty()) {
        for (auto &col : columns.Physical()) {
            names.push_back(Str(col.Name()));
        }
    } else {
        names.resize(plan->types.size());
        idx_t mapped = 0;
        idx_t physical = 0;
        for (auto &col : columns.Physical()) {
            const idx_t source = op.column_index_map[PhysicalIndex(physical++)];
            if (source == DConstants::INVALID_INDEX) {
                continue;
            }
            if (source >= names.size()) {
                throw InternalException("firebird insert: column map out of range");
            }
            names[source] = Str(col.Name());
            ++mapped;
        }
        if (mapped == 0) {
            throw BinderException("INSERT into Firebird table \"%s\": DEFAULT VALUES is not supported; use "
                                  "firebird_execute() with INSERT ... DEFAULT VALUES",
                                  Str(table.name));
        }
    }
    if (names.size() != plan->types.size()) {
        throw InternalException("firebird insert: %llu columns for %llu values",
                                static_cast<unsigned long long>(names.size()),
                                static_cast<unsigned long long>(plan->types.size()));
    }

    std::string sql = "INSERT INTO " + QuoteIdent(Str(table.name)) + " (";
    std::string marks;
    for (idx_t i = 0; i < names.size(); ++i) {
        sql += (i ? ", " : "") + QuoteIdent(names[i]);
        marks += i ? ", ?" : "?";
    }
    sql += ") VALUES (" + marks + ")";

    auto &insert = planner.Make<FirebirdInsert>(op.types, op.estimated_cardinality, catalog, Str(table.name), sql,
                                                plan->types, none_encoding);
    insert.children.push_back(*plan);
    return insert;
}

} // namespace duckdb
