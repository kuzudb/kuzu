#pragma once

#include "catalog/catalog.h"
#include "storage/index/hash_index.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/store/node_group.h"
#include "storage/store/table_data.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace catalog {
class NodeTableSchema;
}
namespace storage {

class NodeTable {
public:
    NodeTable(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        NodesStoreStatsAndDeletedIDs* nodesStatisticsAndDeletedIDs, BufferManager& bufferManager,
        WAL* wal, catalog::NodeTableSchema* nodeTableSchema, bool enableCompression);

    void initializePKIndex(catalog::NodeTableSchema* nodeTableSchema);

    inline common::offset_t getMaxNodeOffset(transaction::Transaction* transaction) const {
        return nodesStatisticsAndDeletedIDs->getMaxNodeOffset(transaction, tableID);
    }
    inline void setSelVectorForDeletedOffsets(
        transaction::Transaction* trx, std::shared_ptr<common::ValueVector>& vector) const {
        assert(vector->isSequential());
        nodesStatisticsAndDeletedIDs->setDeletedNodeOffsetsForMorsel(trx, vector, tableID);
    }

    inline BMFileHandle* getDataFH() const { return tableData->getDataFH(); }

    inline void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors) {
        tableData->read(transaction, nodeIDVector, columnIDs, outputVectors);
    }
    void insert(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    inline void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* nodeIDVector, common::ValueVector* propertyVector) {
        tableData->update(transaction, columnID, nodeIDVector, propertyVector);
    }
    inline void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::offset_t nodeOffset, common::ValueVector* propertyVector,
        common::sel_t posInPropertyVector) const {
        tableData->update(transaction, columnID, nodeOffset, propertyVector, posInPropertyVector);
    }
    void delete_(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* pkVector);
    inline void append(NodeGroup* nodeGroup) { tableData->append(nodeGroup); }

    inline common::column_id_t getNumColumns() const { return tableData->getNumColumns(); }
    inline NodeColumn* getColumn(common::column_id_t columnID) {
        return tableData->getColumn(columnID);
    }
    inline common::column_id_t getPKColumnID() const { return pkColumnID; }
    inline PrimaryKeyIndex* getPKIndex() const { return pkIndex.get(); }
    inline NodesStoreStatsAndDeletedIDs* getNodeStatisticsAndDeletedIDs() const {
        return nodesStatisticsAndDeletedIDs;
    }
    inline common::table_id_t getTableID() const { return tableID; }

    inline void dropColumn(common::column_id_t columnID) { tableData->dropColumn(columnID); }
    void addColumn(const catalog::Property& property, common::ValueVector* defaultValueVector,
        transaction::Transaction* transaction);

    void prepareCommit();
    void prepareRollback();
    void checkpointInMemory();
    void rollbackInMemory();

    inline bool compressionEnabled() const { return tableData->compressionEnabled(); }

private:
    void insertPK(common::ValueVector* nodeIDVector, common::ValueVector* primaryKeyVector);
    inline uint64_t getNumNodeGroups(transaction::Transaction* transaction) const {
        return tableData->getNumNodeGroups(transaction);
    }

private:
    NodesStoreStatsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    std::unique_ptr<TableData> tableData;
    common::column_id_t pkColumnID;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
    common::table_id_t tableID;
    BufferManager& bufferManager;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
