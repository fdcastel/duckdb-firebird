// Cancelling a running Firebird request when the DuckDB query is
// interrupted. See firebird_cancel.hpp.

#include "firebird_cancel.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

#include "firebird_client.hpp"

namespace duckdb {

struct FirebirdCancelGuard::Entry {
    ClientContext *context = nullptr;
    FirebirdConnection *conn = nullptr;
    bool cancelled = false;
};

namespace {

constexpr auto kPollInterval = std::chrono::milliseconds(100);

class CancelWatcher {
public:
    void Add(const std::shared_ptr<FirebirdCancelGuard::Entry> &entry) {
        std::lock_guard<std::mutex> g(lock_);
        if (!started_) {
            // Detached: the watcher lives as long as the process (see Get).
            std::thread([this] { Run(); }).detach();
            started_ = true;
        }
        entries_.push_back(entry);
        wake_.notify_one();
    }

    void Remove(const std::shared_ptr<FirebirdCancelGuard::Entry> &entry) {
        std::lock_guard<std::mutex> g(lock_);
        entries_.erase(std::remove(entries_.begin(), entries_.end(), entry), entries_.end());
    }

    bool Cancelled(const FirebirdCancelGuard::Entry &entry) {
        std::lock_guard<std::mutex> g(lock_);
        return entry.cancelled;
    }

    // Never destroyed: the detached thread may still run while static
    // objects are destroyed at process exit.
    static CancelWatcher &Get() {
        static CancelWatcher *watcher = new CancelWatcher();
        return *watcher;
    }

private:
    void Run() {
        std::unique_lock<std::mutex> g(lock_);
        while (true) {
            if (entries_.empty()) {
                wake_.wait(g);
                continue;
            }
            for (auto &entry : entries_) {
                if (!entry->cancelled && entry->conn && entry->context->IsInterrupted()) {
                    entry->cancelled = true;
                    try {
                        entry->conn->CancelOperation();
                    } catch (...) {
                    }
                }
            }
            wake_.wait_for(g, kPollInterval);
        }
    }

    std::mutex lock_;
    std::condition_variable wake_;
    std::vector<std::shared_ptr<FirebirdCancelGuard::Entry>> entries_;
    bool started_ = false;
};

} // namespace

FirebirdCancelGuard::FirebirdCancelGuard(ClientContext &context, FirebirdConnection *conn)
    : entry_(std::make_shared<Entry>()) {
    entry_->context = &context;
    entry_->conn = conn;
    CancelWatcher::Get().Add(entry_);
}

FirebirdCancelGuard::~FirebirdCancelGuard() {
    CancelWatcher::Get().Remove(entry_);
}

bool FirebirdCancelGuard::Cancelled() const {
    return CancelWatcher::Get().Cancelled(*entry_);
}

void ThrowIfFirebirdCancelled(const FirebirdCancelGuard &guard) {
    if (guard.Cancelled()) {
        throw InterruptException();
    }
}

} // namespace duckdb
