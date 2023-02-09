#pragma once

#include <memory>
#include <thread>

#include "common/api.h"
#include "common/configs.h"
#include "kuzu_fwd.h"

namespace kuzu {
namespace main {

KUZU_API struct SystemConfig {
    explicit SystemConfig();
    explicit SystemConfig(uint64_t bufferPoolSize);

    uint64_t defaultPageBufferPoolSize;
    uint64_t largePageBufferPoolSize;
    uint64_t maxNumThreads;
};

KUZU_API struct DatabaseConfig {
    explicit DatabaseConfig(std::string databasePath);

    std::string databasePath;
};

class Database {
    friend class EmbeddedShell;
    friend class Connection;
    friend class JOConnection;
    friend class kuzu::testing::BaseGraphTest;

public:
    KUZU_API explicit Database(DatabaseConfig databaseConfig);
    KUZU_API Database(DatabaseConfig databaseConfig, SystemConfig systemConfig);
    KUZU_API ~Database();

    void setLoggingLevel(std::string loggingLevel);

    KUZU_API void resizeBufferManager(uint64_t newSize);

private:
    // Commits and checkpoints a write transaction or rolls that transaction back. This involves
    // either replaying the WAL and either redoing or undoing and in either case at the end WAL is
    // cleared.
    // skipCheckpointForTestingRecovery is used to simulate a failure before checkpointing in tests.
    void commitAndCheckpointOrRollback(transaction::Transaction* writeTransaction, bool isCommit,
        bool skipCheckpointForTestingRecovery = false);

    void initDBDirAndCoreFilesIfNecessary() const;
    void initLoggers();

    void checkpointAndClearWAL();
    void rollbackAndClearWAL();
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
    std::unique_ptr<storage::WAL> wal;
    std::shared_ptr<spdlog::logger> logger;
};

} // namespace main
} // namespace kuzu
