#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "common/api.h"
#include "common/case_insensitive_map.h"
#include "kuzu_fwd.h"
#include "main/db_config.h"

namespace kuzu {
namespace common {
class FileSystem;
enum class LogicalTypeID : uint8_t;
} // namespace common

namespace catalog {
class CatalogEntry;
} // namespace catalog

namespace function {
struct Function;
} // namespace function

namespace extension {
struct ExtensionUtils;
struct ExtensionOptions;
} // namespace extension

namespace storage {
class StorageExtension;
} // namespace storage

namespace main {
struct ExtensionOption;
class DatabaseManager;
class ClientContext;

/**
 * @brief Stores runtime configuration for creating or opening a Database
 */
struct KUZU_API SystemConfig {
    /**
     * @brief Creates a SystemConfig object.
     * @param bufferPoolSize Max size of the buffer pool in bytes.
     *        The larger the buffer pool, the more data from the database files is kept in memory,
     *        reducing the amount of File I/O
     * @param maxNumThreads The maximum number of threads to use during query execution
     * @param enableCompression Whether or not to compress data on-disk for supported types
     * @param readOnly If true, the database is opened read-only. No write transaction is
     * allowed on the `Database` object. Multiple read-only `Database` objects can be created with
     * the same database path. If false, the database is opened read-write. Under this mode,
     * there must not be multiple `Database` objects created with the same database path.
     * @param maxDBSize The maximum size of the database in bytes. Note that this is introduced
     * temporarily for now to get around with the default 8TB mmap address space limit some
     * environment. This will be removed once we implemente a better solution later. The value is
     * default to 1 << 43 (8TB) under 64-bit environment and 1GB under 32-bit one (see
     * `DEFAULT_VM_REGION_MAX_SIZE`).
     * @param autoCheckpoint If true, the database will automatically checkpoint when the size of
     * the WAL file exceeds the checkpoint threshold.
     * @param checkpointThreshold The threshold of the WAL file size in bytes. When the size of the
     * WAL file exceeds this threshold, the database will checkpoint if autoCheckpoint is true.
     */
    explicit SystemConfig(uint64_t bufferPoolSize = -1u, uint64_t maxNumThreads = 0,
        bool enableCompression = true, bool readOnly = false, uint64_t maxDBSize = -1u,
        bool autoCheckpoint = true, uint64_t checkpointThreshold = 16777216 /* 16MB */);

    uint64_t bufferPoolSize;
    uint64_t maxNumThreads;
    bool enableCompression;
    bool readOnly;
    uint64_t maxDBSize;
    bool autoCheckpoint;
    uint64_t checkpointThreshold;
};

/**
 * @brief Database class is the main class of KùzuDB. It manages all database components.
 */
class Database {
    friend class EmbeddedShell;
    friend class ClientContext;
    friend class Connection;
    friend class StorageDriver;
    friend class testing::BaseGraphTest;
    friend class testing::PrivateGraphTest;
    friend class transaction::TransactionContext;
    friend struct extension::ExtensionUtils;

public:
    /**
     * @brief Creates a database object.
     * @param databasePath Database path. If left empty, or :memory: is specified, this will create
     *        an in-memory database.
     * @param systemConfig System configurations (buffer pool size and max num threads).
     */
    KUZU_API explicit Database(std::string_view databasePath,
        SystemConfig systemConfig = SystemConfig());
    /**
     * @brief Destructs the database object.
     */
    KUZU_API ~Database();

    // TODO(Ziyi): Instead of exposing a dedicated API for adding a new function, we should consider
    // add function through the extension module.
    void addTableFunction(std::string name,
        std::vector<std::unique_ptr<function::Function>> functionSet);

    KUZU_API void registerFileSystem(std::unique_ptr<common::FileSystem> fs);

    KUZU_API void registerStorageExtension(std::string name,
        std::unique_ptr<storage::StorageExtension> storageExtension);

    KUZU_API void addExtensionOption(std::string name, common::LogicalTypeID type,
        common::Value defaultValue);

    KUZU_API catalog::Catalog* getCatalog() { return catalog.get(); }

    ExtensionOption* getExtensionOption(std::string name) const;

    const DBConfig& getConfig() const { return dbConfig; }

    common::case_insensitive_map_t<std::unique_ptr<storage::StorageExtension>>&
    getStorageExtensions();

    uint64_t getNextQueryID();

private:
    struct QueryIDGenerator {
        uint64_t queryID = 0;
        std::mutex queryIDLock;
    };

    void openLockFile();
    void initAndLockDBDir();

private:
    std::string databasePath;
    DBConfig dbConfig;
    std::unique_ptr<common::VirtualFileSystem> vfs;
    std::unique_ptr<storage::BufferManager> bufferManager;
    std::unique_ptr<storage::MemoryManager> memoryManager;
    std::unique_ptr<processor::QueryProcessor> queryProcessor;
    std::unique_ptr<catalog::Catalog> catalog;
    std::unique_ptr<storage::StorageManager> storageManager;
    std::unique_ptr<transaction::TransactionManager> transactionManager;
    std::unique_ptr<common::FileInfo> lockFile;
    std::unique_ptr<extension::ExtensionOptions> extensionOptions;
    std::unique_ptr<DatabaseManager> databaseManager;
    common::case_insensitive_map_t<std::unique_ptr<storage::StorageExtension>> storageExtensions;
    QueryIDGenerator queryIDGenerator;
};

} // namespace main
} // namespace kuzu
