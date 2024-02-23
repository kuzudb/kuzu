#pragma once

#include <utility>

#include "common/assert.h"
#include "common/cast.h"
#include "storage/index/hash_index.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/store/node_group.h"
#include "storage/store/node_table_data.h"
#include "storage/store/table.h"

namespace kuzu {
namespace catalog {
class NodeTableSchema;
}
namespace storage {

class NodeTable final : public Table {
public:
    NodeTable(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        catalog::NodeTableCatalogEntry* nodeTableEntry,
        NodesStoreStatsAndDeletedIDs* nodesStatisticsAndDeletedIDs, MemoryManager* memoryManager,
        WAL* wal, bool readOnly, bool enableCompression, common::VirtualFileSystem* vfs);

    void initializePKIndex(catalog::NodeTableCatalogEntry* nodeTableEntry, bool readOnly,
        common::VirtualFileSystem* vfs);

    inline common::offset_t getMaxNodeOffset(transaction::Transaction* transaction) const {
        auto nodesStats = common::ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(
            tablesStatistics);
        return nodesStats->getMaxNodeOffset(transaction, tableID);
    }
    inline void setSelVectorForDeletedOffsets(
        transaction::Transaction* trx, std::shared_ptr<common::ValueVector>& vector) const {
        KU_ASSERT(vector->isSequential());
        common::ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(tablesStatistics)
            ->setDeletedNodeOffsetsForMorsel(trx, vector, tableID);
    }

    inline void initializeReadState(transaction::Transaction* transaction,
        std::vector<common::column_id_t> columnIDs, common::ValueVector* inNodeIDVector,
        TableReadState* readState) {
        tableData->initializeReadState(
            transaction, std::move(columnIDs), inNodeIDVector, readState);
    }
    void read(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) override;

    // Return the max node offset during insertions.
    common::offset_t validateUniquenessConstraint(
        transaction::Transaction* tx, const std::vector<common::ValueVector*>& propertyVectors);
    common::offset_t insert(transaction::Transaction* tx, common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* nodeIDVector, common::ValueVector* propertyVector);
    void delete_(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* pkVector);
    inline void append(NodeGroup* nodeGroup) { tableData->append(nodeGroup); }

    inline common::column_id_t getNumColumns() const { return tableData->getNumColumns(); }
    inline Column* getColumn(common::column_id_t columnID) {
        return tableData->getColumn(columnID);
    }
    inline common::column_id_t getPKColumnID() const { return pkColumnID; }
    inline PrimaryKeyIndex* getPKIndex() const { return pkIndex.get(); }
    inline NodesStoreStatsAndDeletedIDs* getNodeStatisticsAndDeletedIDs() const {
        return common::ku_dynamic_cast<TablesStatistics*, NodesStoreStatsAndDeletedIDs*>(
            tablesStatistics);
    }

    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector) override;
    inline void dropColumn(common::column_id_t columnID) override {
        tableData->dropColumn(columnID);
    }

    void prepareCommit(transaction::Transaction* transaction, LocalTable* localTable) override;
    void prepareRollback(LocalTableData* localTable) override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

private:
    void updatePK(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* nodeIDVector, common::ValueVector* pkVector);
    void insertPK(common::ValueVector* nodeIDVector, common::ValueVector* primaryKeyVector);

private:
    std::unique_ptr<NodeTableData> tableData;
    common::column_id_t pkColumnID;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
};

} // namespace storage
} // namespace kuzu
