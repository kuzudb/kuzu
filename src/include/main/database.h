#pragma once

#include <memory>
#include <string>

#include "common/api.h"
#include "common/enums/access_mode.h"
#include "kuzu_fwd.h"

namespace kuzu {
namespace main {

/**
 * @brief Stores runtime configuration for creating or opening a Database
 */
struct KUZU_API SystemConfig {
    /**
     * @brief Creates a SystemConfig object.
     * @param bufferPoolSize Max size of the buffer pool in bytes.
     *        The larger the buffer pool, the more data from the database files is kept in memory,
     *        reducing the amount of File I/O
     *  @param maxNumThreads The maximum number of threads to use during query execution
     *  @param enableCompression Whether or not to compress data on-disk for supported types
     *  @param accessMode Access mode to the database (READ_ONLY or READ_WRITE)
     */
    explicit SystemConfig(uint64_t bufferPoolSize = -1u, uint64_t maxNumThreads = 0,
        bool enableCompression = true,
        common::AccessMode accessMode = common::AccessMode::READ_WRITE);

    uint64_t bufferPoolSize;
    uint64_t maxNumThreads;
    bool enableCompression;
    common::AccessMode accessMode;
};

/**
 * @brief Database class is the main class of KùzuDB. It manages all database components.
 */
class Database {
    friend class EmbeddedShell;
    friend class Connection;
    friend class StorageDriver;
    friend class kuzu::testing::BaseGraphTest;
    friend class kuzu::testing::PrivateGraphTest;
    friend class transaction::TransactionContext;

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
    KUZU_API static void setLoggingLevel(std::string loggingLevel);

private:
    void openLockFile();
    void initDBDirAndCoreFilesIfNecessary();
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
    std::unique_ptr<common::FileInfo> lockFile;
};

} // namespace main
} // namespace kuzu
