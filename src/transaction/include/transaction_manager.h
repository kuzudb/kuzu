#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>

#include "transaction.h"

#include "src/common/include/utils.h"

using namespace std;
using lock_t = unique_lock<mutex>;

namespace graphflow {
namespace transaction {

class TransactionManager {

public:
    TransactionManager()
        : activeWriteTransactionID{INT64_MAX}, lastTransactionID{0}, lastCommitID{0} {};
    unique_ptr<Transaction> beginWriteTransaction();
    unique_ptr<Transaction> beginReadOnlyTransaction();
    void commit(Transaction* transaction);
    void rollback(Transaction* transaction);

    // Warning: Below public functions are for tests only
    inline unordered_set<uint64_t>& getActiveReadOnlyTransactionIDs() {
        lock_t lck{mtx};
        return activeReadOnlyTransactionIDs;
    }
    inline uint64_t getActiveWriteTransactionID() {
        lock_t lck{mtx};
        return activeWriteTransactionID;
    }
    inline bool hasActiveWriteTransactionID() {
        lock_t lck{mtx};
        return activeWriteTransactionID != INT64_MAX;
    }

private:
    inline bool hasActiveWriteTransactionNoLock() { return activeWriteTransactionID != INT64_MAX; }
    inline void clearActiveWriteTransactionNoLock() { activeWriteTransactionID = INT64_MAX; }
    void commitOrRollback(Transaction* transaction, bool isCommit);

private:
    shared_ptr<spdlog::logger> logger;

    uint64_t activeWriteTransactionID;

    unordered_set<uint64_t> activeReadOnlyTransactionIDs;

    uint64_t lastTransactionID;

    // ID of the last committed write transaction. This is currently used primarily for
    // debugging purposes during development and is not written to disk in a db file.
    // In particular, transactions do not use this to perform reads. Our current transaction design
    // supports a concurrency model that requires on 2 versions, one for the read-only transactions
    // and the for the writer transaction, so we can read correct version by looking at the type of
    // the transaction.
    uint64_t lastCommitID;
    mutex mtx;
};
} // namespace transaction
} // namespace graphflow
