#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

// Returns a fresh StorageExtension instance to register with DuckDB's
// extension loader. Hooking this up makes
//
//     ATTACH 'firebird://SYSDBA:masterkey@host/path/db.fdb' AS fb (TYPE firebird);
//     SELECT * FROM fb.main.EMPLOYEE WHERE …;
//
// work as a federated read-only catalog.
unique_ptr<StorageExtension> GetFirebirdStorageExtension();

// firebird_pool_stats(catalog_name VARCHAR) — factual connection-pool
// introspection for one attached Firebird catalog. Emits a single row of
// pool config + idle-queue size + lifetime counters. Reads only what the
// pool already tracks; never leases a connection.
TableFunction GetFirebirdPoolStatsFunction();

// PROTOTYPE (attach-wide snapshot): the Firebird snapshot number pinned by
// the current DuckDB transaction on this attached catalog, started on
// first use. 0 when no snapshot can be shared (Firebird 3, or it could
// not be started).
struct FirebirdConnectionInfo;
class FirebirdConnectionPool;
int64_t FirebirdTransactionSnapshotNumber(ClientContext &context, Catalog &catalog,
                                          const FirebirdConnectionInfo &info,
                                          const std::shared_ptr<FirebirdConnectionPool> &pool);

} // namespace duckdb
