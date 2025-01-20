#pragma once

#include <mutex>

#include "catalog/catalog.h"
#include "storage/index/hash_index.h"
#include "storage/wal/shadow_file.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace main {
class Database;
} // namespace main

namespace catalog {
class CatalogEntry;
}

namespace storage {
class Table;
class DiskArrayCollection;

class StorageManager {
public:
    StorageManager(const std::string& databasePath, bool readOnly, const catalog::Catalog& catalog,
        MemoryManager& memoryManager, bool enableCompression, common::VirtualFileSystem* vfs,
        main::ClientContext* context);
    ~StorageManager();

    static void recover(main::ClientContext& clientContext);

    void createTable(catalog::CatalogEntry* entry, main::ClientContext* context);

    void checkpoint(main::ClientContext& clientContext);
    void rollbackCheckpoint(main::ClientContext& clientContext);

    Table* getTable(common::table_id_t tableID) {
        std::lock_guard lck{mtx};
        KU_ASSERT(tables.contains(tableID));
        return tables.at(tableID).get();
    }

    WAL& getWAL() const;
    ShadowFile& getShadowFile() const;
    FileHandle* getDataFH() const { return dataFH; }
    std::string getDatabasePath() const { return databasePath; }
    bool isReadOnly() const { return readOnly; }
    bool compressionEnabled() const { return enableCompression; }

private:
    FileHandle* initFileHandle(const std::string& fileName, common::VirtualFileSystem* vfs,
        main::ClientContext* context) const;

    void loadTables(const catalog::Catalog& catalog, common::VirtualFileSystem* vfs,
        main::ClientContext* context);
    void createNodeTable(catalog::NodeTableCatalogEntry* entry, main::ClientContext* context);
    void createRelTable(catalog::RelTableCatalogEntry* entry);
    void createRelTableGroup(catalog::RelGroupCatalogEntry* entry, main::ClientContext* context);

private:
    std::mutex mtx;
    std::string databasePath;
    bool readOnly;
    FileHandle* dataFH;
    FileHandle* metadataFH;
    std::unordered_map<common::table_id_t, std::unique_ptr<Table>> tables;
    MemoryManager& memoryManager;
    std::unique_ptr<WAL> wal;
    std::unique_ptr<ShadowFile> shadowFile;
    bool enableCompression;
};

} // namespace storage
} // namespace kuzu
