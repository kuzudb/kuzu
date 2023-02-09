#pragma once

#include <memory>
#include <thread>

#include "common/api.h"
#include "common/configs.h"
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
     * @param bufferPoolSize Buffer pool size in bytes.
     * @note defaultPageBufferPoolSize and largePageBufferPoolSize are calculated based on the
     * DEFAULT_PAGES_BUFFER_RATIO and LARGE_PAGES_BUFFER_RATIO constants in StorageConfig.
     */
    explicit SystemConfig(uint64_t bufferPoolSize);

    uint64_t defaultPageBufferPoolSize;
    uint64_t largePageBufferPoolSize;
    uint64_t maxNumThreads;
};

/**
 * @brief Stores databasePath.
 */
KUZU_API struct DatabaseConfig {
    /**
     * @brief Creates a DatabaseConfig object.
     * @param databasePath Path to store the database files.
     */
    explicit DatabaseConfig(std::string databasePath);

    std::string databasePath;
};

/**
 * @brief Database class is the main class of the KuzuDB. It manages all database configurations and
 * files.
 */
class Database {
    friend class EmbeddedShell;
    friend class Connection;
    friend class JOConnection;
    friend class kuzu::testing::BaseGraphTest;

public:
    /**
     * @brief Creates a database object with default buffer pool size and max num threads.
     * @param databaseConfig Database configurations(database path).
     */
    KUZU_API explicit Database(DatabaseConfig databaseConfig);
    /**
     * @brief Creates a database object.
     * @param databaseConfig Database configurations(database path).
     * @param systemConfig System configurations(buffer pool size and max num threads).
     */
    KUZU_API Database(DatabaseConfig databaseConfig, SystemConfig systemConfig);
    /**
     * @brief Deconstructs the database object.
     */
    KUZU_API ~Database();

    /**
     * @brief Sets the logging level of the database instance.
     * @param loggingLevel New logging level. (Supported logging levels are: "info", "debug",
     * "err").
     */
    void setLoggingLevel(std::string loggingLevel);

    /**
     * @brief Resizes the buffer pool size of the database instance.
     * @param newSize New buffer pool size in bytes.
     * @throws BufferManagerException if the new size is smaller than the current buffer manager
     * size.
     */
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
