#pragma once

#include <mutex>

#include "catalog/catalog.h"
#include "storage/index/hash_index.h"
#include "storage/index/hnsw_index.h"
#include "storage/wal/shadow_file.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace main {
class Database;
} // namespace main

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

    void createTable(common::table_id_t tableID, const catalog::Catalog* catalog,
        main::ClientContext* context);

    void createHNNSWIndex(std::string name, std::unique_ptr<InMemHNSWIndex> hnswIndex) {
        std::lock_guard lck{mtx};
        KU_ASSERT(!hnswIndexes.contains(name));
        hnswIndexes[name] = std::move(hnswIndex);
    }
    InMemHNSWIndex* getHNNSWIndex(const std::string& name) {
        std::lock_guard lck{mtx};
        KU_ASSERT(hnswIndexes.contains(name));
        return hnswIndexes.at(name).get();
    }

    void checkpoint(main::ClientContext& clientContext);

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

    // TODO(Guodong): This is a temp hack. Should be private.
    void createRelTable(common::table_id_t tableID, catalog::RelTableCatalogEntry* relTableEntry);

private:
    FileHandle* initFileHandle(const std::string& fileName, common::VirtualFileSystem* vfs,
        main::ClientContext* context) const;

    void loadTables(const catalog::Catalog& catalog, common::VirtualFileSystem* vfs,
        main::ClientContext* context);
    void createNodeTable(common::table_id_t tableID, catalog::NodeTableCatalogEntry* nodeTableEntry,
        main::ClientContext* context);
    void createRelTableGroup(common::table_id_t tableID,
        const catalog::RelGroupCatalogEntry* tableSchema, const catalog::Catalog* catalog,
        transaction::Transaction* transaction);

private:
    std::mutex mtx;
    std::string databasePath;
    bool readOnly;
    FileHandle* dataFH;
    FileHandle* metadataFH;
    std::unordered_map<common::table_id_t, std::unique_ptr<Table>> tables;
    std::unordered_map<std::string, std::unique_ptr<InMemHNSWIndex>> hnswIndexes;
    MemoryManager& memoryManager;
    std::unique_ptr<WAL> wal;
    std::unique_ptr<ShadowFile> shadowFile;
    bool enableCompression;
};

} // namespace storage
} // namespace kuzu
