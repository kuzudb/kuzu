#pragma once

#include <mutex>

#include "catalog/catalog.h"
#include "shadow_file.h"
#include "storage/index/hash_index.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace main {
class Database;
} // namespace main

namespace catalog {
class CatalogEntry;
}

namespace storage {
class DiskArrayCollection;
class Table;

class KUZU_API StorageManager {
public:
    StorageManager(const std::string& databasePath, bool readOnly, MemoryManager& memoryManager,
        bool enableCompression, common::VirtualFileSystem* vfs, main::ClientContext* context);
    ~StorageManager();

    static void recover(main::ClientContext& clientContext);

    void createTable(catalog::CatalogEntry* entry, const main::ClientContext* context);

    void checkpoint(const catalog::Catalog& catalog);
    void finalizeCheckpoint();
    void rollbackCheckpoint(const catalog::Catalog& catalog);

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
    bool isInMemory() const { return inMemory; }

    void serialize(const catalog::Catalog& catalog, common::Serializer& ser);
    void deserialize(const catalog::Catalog& catalog, common::Deserializer& deSer);

private:
    void initDataFileHandle(common::VirtualFileSystem* vfs, main::ClientContext* context);

    void createNodeTable(catalog::NodeTableCatalogEntry* entry);
    void createRelTable(catalog::RelTableCatalogEntry* entry);
    void createRelTableGroup(const catalog::RelGroupCatalogEntry* entry,
        const main::ClientContext* context);

    void reclaimDroppedTables(const catalog::Catalog& catalog);

private:
    std::mutex mtx;
    std::string databasePath;
    bool readOnly;
    FileHandle* dataFH;
    std::unordered_map<common::table_id_t, std::unique_ptr<Table>> tables;
    MemoryManager& memoryManager;
    std::unique_ptr<WAL> wal;
    std::unique_ptr<ShadowFile> shadowFile;
    bool enableCompression;
    bool inMemory;
};

} // namespace storage
} // namespace kuzu
