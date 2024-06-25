#pragma once

#include "catalog/catalog.h"
#include "storage/index/hash_index.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/stats/rels_store_statistics.h"
#include "storage/store/rel_table.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace main {
class Database;
} // namespace main

namespace storage {

class DiskArrayCollection;

class StorageManager {
public:
    StorageManager(const std::string& databasePath, bool readOnly, const catalog::Catalog& catalog,
        MemoryManager& memoryManager, bool enableCompression, common::VirtualFileSystem* vfs,
        main::ClientContext* context);

    static void recover(main::ClientContext& clientContext);

    void createTable(common::table_id_t tableID, catalog::Catalog* catalog,
        main::ClientContext* context);
    void dropTable(common::table_id_t tableID, common::VirtualFileSystem* vfs);

    void prepareCommit(transaction::Transaction* transaction, common::VirtualFileSystem* vfs);
    void prepareRollback();
    void checkpointInMemory();
    void rollbackInMemory();

    PrimaryKeyIndex* getPKIndex(common::table_id_t tableID);

    Table* getTable(common::table_id_t tableID) const {
        KU_ASSERT(tables.contains(tableID));
        return tables.at(tableID).get();
    }

    WAL& getWAL();
    BMFileHandle* getDataFH() const { return dataFH; }
    BMFileHandle* getMetadataFH() const { return metadataFH; }
    DiskArrayCollection* getMetadataDAC() const { return metadataDAC.get(); }
    void initStatistics() {
        nodesStatisticsAndDeletedIDs->initTableStatisticsForWriteTrx();
        relsStatistics->initTableStatisticsForWriteTrx();
    }
    NodesStoreStatsAndDeletedIDs* getNodesStatisticsAndDeletedIDs() {
        return nodesStatisticsAndDeletedIDs.get();
    }
    RelsStoreStats* getRelsStatistics() { return relsStatistics.get(); }
    std::string getDatabasePath() const { return databasePath; }
    bool isReadOnly() const { return readOnly; }
    bool compressionEnabled() const { return enableCompression; }

private:
    BMFileHandle* initFileHandle(const std::string& filename, common::VirtualFileSystem* vfs,
        main::ClientContext* context);

    void loadTables(const catalog::Catalog& catalog, common::VirtualFileSystem* vfs,
        main::ClientContext* context);
    void createNodeTable(common::table_id_t tableID, catalog::NodeTableCatalogEntry* tableSchema,
        main::ClientContext* context);
    void createRelTable(common::table_id_t tableID, catalog::RelTableCatalogEntry* tableSchema,
        catalog::Catalog* catalog, transaction::Transaction* transaction);
    void createRelTableGroup(common::table_id_t tableID, catalog::RelGroupCatalogEntry* tableSchema,
        catalog::Catalog* catalog, transaction::Transaction* transaction);
    void createRdfGraph(common::table_id_t tableID, catalog::RDFGraphCatalogEntry* tableSchema,
        catalog::Catalog* catalog, main::ClientContext* context);

private:
    std::string databasePath;
    bool readOnly;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    std::unique_ptr<DiskArrayCollection> metadataDAC;
    std::unique_ptr<NodesStoreStatsAndDeletedIDs> nodesStatisticsAndDeletedIDs;
    std::unique_ptr<RelsStoreStats> relsStatistics;
    std::unordered_map<common::table_id_t, std::unique_ptr<Table>> tables;
    MemoryManager& memoryManager;
    std::unique_ptr<WAL> wal;
    bool enableCompression;
};

} // namespace storage
} // namespace kuzu
