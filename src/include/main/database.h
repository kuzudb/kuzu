#pragma once

// TODO: Consider using forward declaration
#include "common/configs.h"
#include "common/logging_level_utils.h"
#include "processor/processor.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_manager.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"

namespace kuzu {
namespace testing {
class BaseGraphTest;
} // namespace testing
} // namespace kuzu

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
    friend class EmbeddedShell;
    friend class Connection;
    friend class JOConnection;
    friend class kuzu::testing::BaseGraphTest;

public:
    explicit Database(const DatabaseConfig& databaseConfig)
        : Database{databaseConfig, SystemConfig()} {}

    explicit Database(const DatabaseConfig& databaseConfig, const SystemConfig& systemConfig);

    void setLoggingLevel(spdlog::level::level_enum loggingLevel);

    void resizeBufferManager(uint64_t newSize);

    ~Database() = default;

private:
    // TODO(Semih): This is refactored here for now to be able to test transaction behavior
    // in absence of the frontend support. Consider moving this code to connection.cpp.
    // Commits and checkpoints a write transaction or rolls that transaction back. This involves
    // either replaying the WAL and either redoing or undoing and in either case at the end WAL is
    // cleared.
    // skipCheckpointForTestingRecovery is used to simulate a failure before checkpointing in tests.
    void commitAndCheckpointOrRollback(transaction::Transaction* writeTransaction, bool isCommit,
        bool skipCheckpointForTestingRecovery = false);

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
