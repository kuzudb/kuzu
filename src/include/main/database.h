#pragma once

#include <memory>
#include <thread>

#include "common/api.h"
#include "common/constants.h"
#include "kuzu_fwd.h"

namespace kuzu {
namespace main {

/**
 * @brief Stores buffer pool size and max number of threads configurations.
 */
KUZU_API struct SystemConfig {
    /**
     * @brief Creates a SystemConfig object with default buffer pool size and max num of threads.
     */
    explicit SystemConfig();
    /**
     * @brief Creates a SystemConfig object.
     * @param bufferPoolSize Max size of the buffer pool in bytes.
     */
    explicit SystemConfig(uint64_t bufferPoolSize);

    uint64_t bufferPoolSize;
    uint64_t maxNumThreads;
};

/**
 * @brief Database class is the main class of KùzuDB. It manages all database components.
 */
class Database {
    friend class EmbeddedShell;
    friend class Connection;
    friend class StorageDriver;
    friend class kuzu::testing::BaseGraphTest;

public:
    /**
     * @brief Creates a database object with default buffer pool size and max num threads.
     * @param databaseConfig Database path.
     */
    KUZU_API explicit Database(std::string databasePath);
    /**
     * @brief Creates a database object.
     * @param databasePath Database path.
     * @param systemConfig System configurations (buffer pool size and max num threads).
     */
    KUZU_API Database(std::string databasePath, SystemConfig systemConfig);
    /**
     * @brief Destructs the database object.
     */
    KUZU_API ~Database();

    /**
     * @brief Sets the logging level of the database instance.
     * @param loggingLevel New logging level. (Supported logging levels are: "info", "debug",
     * "err").
     */
    static void setLoggingLevel(std::string loggingLevel);

private:
    void initDBDirAndCoreFilesIfNecessary() const;
    static void initLoggers();
    static void dropLoggers();

    // Commits and checkpoints a write transaction or rolls that transaction back. This involves
    // either replaying the WAL and either redoing or undoing and in either case at the end WAL is
    // cleared.
    // skipCheckpointForTestingRecovery is used to simulate a failure before checkpointing in tests.
    void commit(transaction::Transaction* transaction, bool skipCheckpointForTestingRecovery);
    void rollback(transaction::Transaction* transaction, bool skipCheckpointForTestingRecovery);
    void checkpointAndClearWAL(storage::WALReplayMode walReplayMode);
    void rollbackAndClearWAL();
    void recoverIfNecessary();

private:
    std::string databasePath;
    SystemConfig systemConfig;
    std::unique_ptr<storage::BufferManager> bufferManager;
    std::unique_ptr<storage::MemoryManager> memoryManager;
    std::unique_ptr<processor::QueryProcessor> queryProcessor;
    std::unique_ptr<catalog::Catalog> catalog;
    std::unique_ptr<storage::StorageManager> storageManager;
    std::unique_ptr<transaction::TransactionManager> transactionManager;
    std::unique_ptr<storage::WAL> wal;
    std::shared_ptr<spdlog::logger> logger;
};

} // namespace main
} // namespace kuzu
