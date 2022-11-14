#pragma once

// TODO: Consider using forward declaration
#include "src/common/include/configs.h"
#include "src/processor/include/processor.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/memory_manager.h"
#include "src/storage/include/storage_manager.h"
#include "src/transaction/include/transaction.h"
#include "src/transaction/include/transaction_manager.h"

namespace kuzu {
namespace transaction {
class TinySnbDDLTest;
class TinySnbCopyCSVTransactionTest;
} // namespace transaction
} // namespace kuzu

namespace spdlog {
class logger;
}

namespace kuzu {
namespace main {

struct SystemConfig {
    explicit SystemConfig(uint64_t bufferPoolSize = StorageConfig::DEFAULT_BUFFER_POOL_SIZE)
        : defaultPageBufferPoolSize{(uint64_t)(
              bufferPoolSize * StorageConfig::DEFAULT_PAGES_BUFFER_RATIO)},
          largePageBufferPoolSize{
              (uint64_t)(bufferPoolSize * StorageConfig::LARGE_PAGES_BUFFER_RATIO)} {}

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
    friend class JOConnection;
    friend class kuzu::transaction::TinySnbDDLTest;
    friend class kuzu::transaction::TinySnbCopyCSVTransactionTest;

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
    inline WAL* getWAL() const { return wal.get(); }

    // TODO(Semih): This is refactored here for now to be able to test transaction behavior
    // in absence of the frontend support. Consider moving this code to connection.cpp.
    // Commits and checkpoints a write transaction or rolls that transaction back. This involves
    // either replaying the WAL and either redoing or undoing and in either case at the end WAL is
    // cleared.
    // skipCheckpointForTestingRecovery is used to simulate a failure before checkpointing in tests.
    void commitAndCheckpointOrRollback(transaction::Transaction* writeTransaction, bool isCommit,
        bool skipCheckpointForTestingRecovery = false);

private:
    void initDBDirAndCoreFilesIfNecessary() const;
    void initLoggers();

    inline void checkpointAndClearWAL() {
        checkpointOrRollbackAndClearWAL(false /* is not recovering */, true /* isCheckpoint */);
    }
    inline void rollbackAndClearWAL() {
        checkpointOrRollbackAndClearWAL(
            false /* is not recovering */, false /* rolling back updates */);
    }
    void recoverIfNecessary();
    void checkpointOrRollbackAndClearWAL(bool isRecovering, bool isCheckpoint);

private:
    DatabaseConfig databaseConfig;
    SystemConfig systemConfig;
    std::unique_ptr<storage::MemoryManager> memoryManager;
    std::unique_ptr<processor::QueryProcessor> queryProcessor;
    std::unique_ptr<storage::BufferManager> bufferManager;
    std::unique_ptr<catalog::Catalog> catalog;
    std::unique_ptr<storage::StorageManager> storageManager;
    std::unique_ptr<transaction::TransactionManager> transactionManager;
    unique_ptr<storage::WAL> wal;
    shared_ptr<spdlog::logger> logger;
};

} // namespace main
} // namespace kuzu
