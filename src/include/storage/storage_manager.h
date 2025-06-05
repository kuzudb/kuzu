#pragma once

#include <mutex>

#include "catalog/catalog.h"
#include "shadow_file.h"
#include "storage/index/index.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace main {
class Database;
} // namespace main

namespace catalog {
class CatalogEntry;
class NodeTableCatalogEntry;
class RelGroupCatalogEntry;
struct RelTableCatalogInfo;
} // namespace catalog

namespace storage {
class Table;
class NodeTable;
class RelTable;
class DiskArrayCollection;

class KUZU_API StorageManager {
public:
    StorageManager(const std::string& databasePath, bool readOnly, MemoryManager& memoryManager,
        bool enableCompression, common::VirtualFileSystem* vfs, main::ClientContext* context);
    ~StorageManager();

    Table* getTable(common::table_id_t tableID);

    static void recover(main::ClientContext& clientContext);

    void createTable(catalog::TableCatalogEntry* entry);
    void addRelTable(catalog::RelGroupCatalogEntry* entry,
        const catalog::RelTableCatalogInfo& info);

    bool checkpoint(main::ClientContext* context);
    void finalizeCheckpoint();
    void rollbackCheckpoint(const catalog::Catalog& catalog);

    WAL& getWAL() const;
    ShadowFile& getShadowFile() const;
    FileHandle* getDataFH() const { return dataFH; }
    std::string getDatabasePath() const { return databasePath; }
    bool isReadOnly() const { return readOnly; }
    bool compressionEnabled() const { return enableCompression; }
    bool isInMemory() const { return inMemory; }

    void registerIndexType(IndexType indexType) {
        registeredIndexTypes.push_back(std::move(indexType));
    }
    std::optional<std::reference_wrapper<const IndexType>> getIndexType(
        const std::string& typeName) const;

    void serialize(const catalog::Catalog& catalog, common::Serializer& ser);
    // We need to pass in the catalog and storageManager explicitly as they can be from
    // attachedDatabase.
    void deserialize(main::ClientContext* context, const catalog::Catalog* catalog,
        common::Deserializer& deSer);

private:
    void initDataFileHandle(common::VirtualFileSystem* vfs, main::ClientContext* context);

    void createNodeTable(catalog::NodeTableCatalogEntry* entry);

    void createRelTableGroup(catalog::RelGroupCatalogEntry* entry);

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
    std::vector<IndexType> registeredIndexTypes;
};

} // namespace storage
} // namespace kuzu
