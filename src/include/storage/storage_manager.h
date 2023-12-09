#pragma once

#include "catalog/catalog.h"
#include "storage/index/hash_index.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/stats/rels_store_statistics.h"
#include "storage/store/node_table.h"
#include "storage/store/rel_table.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace storage {

class StorageManager {
public:
    StorageManager(bool readOnly, const catalog::Catalog& catalog, MemoryManager& memoryManager,
        WAL* wal, bool enableCompression);

    void createTable(common::table_id_t tableID, catalog::Catalog* catalog,
        transaction::Transaction* transaction);
    void dropTable(common::table_id_t tableID);

    void prepareCommit(transaction::Transaction* transaction);
    void prepareRollback(transaction::Transaction* transaction);
    void checkpointInMemory();
    void rollbackInMemory();

    PrimaryKeyIndex* getPKIndex(common::table_id_t tableID);

    inline NodeTable* getNodeTable(common::table_id_t tableID) const {
        KU_ASSERT(tables.contains(tableID) &&
                  tables.at(tableID)->getTableType() == common::TableType::NODE);
        auto table = common::ku_dynamic_cast<Table*, NodeTable*>(tables.at(tableID).get());
        KU_ASSERT(table);
        return table;
    }
    inline RelTable* getRelTable(common::table_id_t tableID) const {
        KU_ASSERT(tables.contains(tableID) &&
                  tables.at(tableID)->getTableType() == common::TableType::REL);
        auto table = common::ku_dynamic_cast<Table*, RelTable*>(tables.at(tableID).get());
        KU_ASSERT(table);
        return table;
    }

    inline WAL* getWAL() const { return wal; }
    inline BMFileHandle* getDataFH() const { return dataFH.get(); }
    inline BMFileHandle* getMetadataFH() const { return metadataFH.get(); }
    inline void initStatistics() {
        nodesStatisticsAndDeletedIDs->initTableStatisticsForWriteTrx();
        relsStatistics->initTableStatisticsForWriteTrx();
    }
    inline NodesStoreStatsAndDeletedIDs* getNodesStatisticsAndDeletedIDs() {
        return nodesStatisticsAndDeletedIDs.get();
    }
    inline RelsStoreStats* getRelsStatistics() { return relsStatistics.get(); }
    inline bool compressionEnabled() const { return enableCompression; }

private:
    void loadTables(bool readOnly, const catalog::Catalog& catalog);

private:
    std::unique_ptr<BMFileHandle> dataFH;
    std::unique_ptr<BMFileHandle> metadataFH;
    std::unique_ptr<NodesStoreStatsAndDeletedIDs> nodesStatisticsAndDeletedIDs;
    std::unique_ptr<RelsStoreStats> relsStatistics;
    std::unordered_map<common::table_id_t, std::unique_ptr<Table>> tables;
    MemoryManager& memoryManager;
    WAL* wal;
    bool enableCompression;
};

} // namespace storage
} // namespace kuzu
