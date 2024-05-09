#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>

#include "storage/wal/wal.h"
#include "transaction.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main

namespace testing {
class DBTest;
} // namespace testing

namespace transaction {

class TransactionManager {
    friend class testing::DBTest;

public:
    // Timestamp starts from 1. 0 is reserved for the dummy system transaction.
    explicit TransactionManager(storage::WAL& wal)
        : wal{wal}, lastTransactionID{Transaction::START_TRANSACTION_ID}, lastTimestamp{1} {};

    // TODO(Guodong): Should rewrite as `beginTransaction`.
    std::unique_ptr<Transaction> beginTransaction(main::ClientContext& clientContext,
        TransactionType type);

    // skipCheckpoint is used to simulate a failure before checkpointing in tests.
    void commit(main::ClientContext& clientContext, bool skipCheckPointing = false);
    void rollback(main::ClientContext& clientContext, Transaction* transaction,
        bool skipCheckPointing = false);
    void checkpoint(main::ClientContext& clientContext);

    // Warning: Below public functions are for tests only
    std::unordered_set<uint64_t>& getActiveReadOnlyTransactionIDs() {
        std::unique_lock<std::mutex> lck{mtxForSerializingPublicFunctionCalls};
        return activeReadOnlyTransactionIDs;
    }
    common::transaction_t getActiveWriteTransactionID() {
        std::unique_lock<std::mutex> lck{mtxForSerializingPublicFunctionCalls};
        KU_ASSERT(activeWriteTransactionID.size() == 1);
        return *activeWriteTransactionID.begin();
    }
    bool hasActiveWriteTransactionID() {
        std::unique_lock<std::mutex> lck{mtxForSerializingPublicFunctionCalls};
        return !activeWriteTransactionID.empty();
    }

private:
    bool canCheckpointNoLock();
    void checkpointNoLock(main::ClientContext& clientContext);
    // This functions locks the mutex to start new transactions. This lock needs to be manually
    // unlocked later by calling allowReceivingNewTransactions() by the thread that called
    // stopNewTransactionsAndWaitUntilAllTransactionsLeave().
    void stopNewTransactionsAndWaitUntilAllTransactionsLeave();
    void allowReceivingNewTransactions();

    bool hasActiveWriteTransactionNoLock() const { return !activeWriteTransactionID.empty(); }
    void clearActiveWriteTransactionIfWriteTransactionNoLock(Transaction* transaction);

    // Note: Used by DBTest::createDB only.
    void setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(uint64_t waitTimeInMicros) {
        checkpointWaitTimeoutInMicros = waitTimeInMicros;
    }

private:
    storage::WAL& wal;
    std::unordered_set<common::transaction_t> activeWriteTransactionID;
    std::unordered_set<uint64_t> activeReadOnlyTransactionIDs;
    common::transaction_t lastTransactionID;
    common::transaction_t lastTimestamp;
    // This mutex is used to ensure thread safety and letting only one public function to be called
    // at any time except the stopNewTransactionsAndWaitUntilAllReadTransactionsLeave
    // function, which needs to let calls to comming and rollback.
    std::mutex mtxForSerializingPublicFunctionCalls;
    std::mutex mtxForStartingNewTransactions;
    uint64_t checkpointWaitTimeoutInMicros = common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_IN_MICROS;
};
} // namespace transaction
} // namespace kuzu
