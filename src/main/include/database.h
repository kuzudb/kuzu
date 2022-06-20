#pragma once

// TODO: Consider using forward declaration
#include "src/common/include/configs.h"
#include "src/processor/include/processor.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/memory_manager.h"
#include "src/storage/include/storage_manager.h"
#include "src/transaction/include/transaction.h"
#include "src/transaction/include/transaction_manager.h"

namespace graphflow {
namespace main {

struct SystemConfig {

    explicit SystemConfig(
        uint64_t defaultPageBufferPoolSize = StorageConfig::DEFAULT_BUFFER_POOL_SIZE *
                                             StorageConfig::DEFAULT_PAGES_BUFFER_RATIO,
        uint64_t largePageBufferPoolSize = StorageConfig::DEFAULT_BUFFER_POOL_SIZE *
                                           StorageConfig::LARGE_PAGES_BUFFER_RATIO)
        : defaultPageBufferPoolSize{defaultPageBufferPoolSize}, largePageBufferPoolSize{
                                                                    largePageBufferPoolSize} {}

    uint64_t defaultPageBufferPoolSize;
    uint64_t largePageBufferPoolSize;

    uint64_t maxNumThreads = std::thread::hardware_concurrency();
};

struct DatabaseConfig {

    explicit DatabaseConfig(std::string databasePath, bool inMemoryMode = false)
        : databasePath{std::move(databasePath)}, inMemoryMode{inMemoryMode} {}

    std::string databasePath;
    bool inMemoryMode;
};

class Database {
    friend class Connection;

public:
    explicit Database(const DatabaseConfig& databaseConfig)
        : Database{databaseConfig, SystemConfig()} {}

    explicit Database(const DatabaseConfig& databaseConfig, const SystemConfig& systemConfig);

    ~Database() = default;

    void resizeBufferManager(uint64_t newSize);

    // TODO: interface below might need to be removed eventually
    inline catalog::Catalog* getCatalog() { return catalog.get(); }
    // used in storage test
    inline storage::StorageManager* getStorageManager() { return storageManager.get(); }
    // used in shell for getting debug info
    inline storage::BufferManager* getBufferManager() { return bufferManager.get(); }
    // Needed for tests currently.
    inline storage::MemoryManager* getMemoryManager() { return memoryManager.get(); }
    // Needed for tests currently.
    inline transaction::TransactionManager* getTransactionManager() {
        return transactionManager.get();
    }
    // used in API test
    inline uint64_t getDefaultBMSize() const { return systemConfig.defaultPageBufferPoolSize; }
    inline uint64_t getLargeBMSize() const { return systemConfig.largePageBufferPoolSize; }

    // TODO(Semih): This is refactored here for now to be able to test transaction behavior
    // in absence of the frontend support. Consider moving this code to connection.cpp.
    // Commits and checkpoints a write transaction or rolls that transaction back. This involves
    // either replaying the WAL and either redoing or undoing and in either case at the end WAL is
    // cleared.
    // skipCheckpointForTestingRecovery is used to simulate a failure before checkpointing in tests.
    inline void commitAndCheckpointOrRollback(transaction::Transaction* writeTransaction,
        bool isCommit, bool skipCheckpointForTestingRecovery = false) {
        // Irrespective of whether we are checkpointing or rolling back we add a nodes_metadata
        // record if there has been updates to NodesMetadata. This is because we need to
        // commit or rollback the in-memory state of NodesMetadata, which is done during wal
        // replaying and committing/rollingback each record, so a NODES_METADATA_RECORD needs to
        // appear in the log.
        if (storageManager->getNodesStore().getNodesMetadata().hasUpdates()) {
            storageManager->getWAL().logNodeMetadataRecord();
            // If we are committing, we also need to write the WAL file for NodesMetadata.
            if (isCommit) {
                storageManager->getNodesStore()
                    .getNodesMetadata()
                    .writeNodesMetadataFileForWALRecord(storageManager->getDBDirectory());
            }
        }
        if (isCommit) {
            // Note: It is enough to stop and wait transactions to leave the system instead of for
            // example checking on the query processor's task scheduler. This is because the first
            // and last steps that a connection performs when executing a query is to start and
            // comming/rollback transaction. The query processor also ensures that it will only
            // return results or error after all threads working on the tasks of a query stop
            // working on the tasks of the query and these tasks are removed from the query.
            transactionManager->stopNewTransactionsAndWaitUntilAllReadTransactionsLeave();
            // Note: committing and stopping new transactions can be done in any order. This order
            // allows us to throw exceptions if we have to wait a lot to stop.
            transactionManager->commitButKeepActiveWriteTransaction(writeTransaction);
            storageManager->getWAL().flushAllPages();
            if (skipCheckpointForTestingRecovery) {
                transactionManager->allowReceivingNewTransactions();
                return;
            }
            storageManager->checkpointAndClearWAL();
        } else {
            if (skipCheckpointForTestingRecovery) {
                storageManager->getWAL().flushAllPages();
                return;
            }
            storageManager->rollbackAndClearWAL();
        }
        transactionManager->manuallyClearActiveWriteTransaction(writeTransaction);
        if (isCommit) {
            transactionManager->allowReceivingNewTransactions();
        }
    }

private:
    DatabaseConfig databaseConfig;
    SystemConfig systemConfig;
    std::unique_ptr<storage::MemoryManager> memoryManager;
    std::unique_ptr<processor::QueryProcessor> queryProcessor;
    std::unique_ptr<storage::BufferManager> bufferManager;
    std::unique_ptr<catalog::Catalog> catalog;
    std::unique_ptr<storage::StorageManager> storageManager;
    std::unique_ptr<transaction::TransactionManager> transactionManager;
};

} // namespace main
} // namespace graphflow
