#pragma once

#include <thread>

#include "../common/configs.h"
#include "forward_declarations.h"
using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace main {

struct SystemConfig {
    explicit SystemConfig(uint64_t bufferPoolSize = StorageConfig::DEFAULT_BUFFER_POOL_SIZE);

    uint64_t defaultPageBufferPoolSize;
    uint64_t largePageBufferPoolSize;

    uint64_t maxNumThreads = std::thread::hardware_concurrency();
};

struct DatabaseConfig {
    explicit DatabaseConfig(std::string databasePath) : databasePath{std::move(databasePath)} {}

    std::string databasePath;
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

    ~Database();

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
