#pragma once

#include "catalog/catalog.h"
#include "storage/index/hash_index.h"
#include "storage/stats/nodes_statistics_and_deleted_ids.h"
#include "storage/storage_structure/lists/lists.h"
#include "storage/store/node_column.h"
#include "storage/store/node_group.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace catalog {
class NodeTableSchema;
}

namespace storage {

class NodeTable {
public:
    struct DeleteState {
        std::unique_ptr<common::ValueVector> pkVector;
    };

    NodeTable(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs, BufferManager& bufferManager,
        WAL* wal, catalog::NodeTableSchema* nodeTableSchema);

    void initializePKIndex(catalog::NodeTableSchema* nodeTableSchema);

    inline common::offset_t getMaxNodeOffset(transaction::Transaction* transaction) const {
        return nodesStatisticsAndDeletedIDs->getMaxNodeOffset(transaction, tableID);
    }

    inline void setSelVectorForDeletedOffsets(
        transaction::Transaction* trx, std::shared_ptr<common::ValueVector>& vector) const {
        assert(vector->isSequential());
        nodesStatisticsAndDeletedIDs->setDeletedNodeOffsetsForMorsel(trx, vector, tableID);
    }
    inline BMFileHandle* getDataFH() const { return dataFH; }

    void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void insert(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* nodeIDVector, common::ValueVector* propertyVector);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::offset_t nodeOffset, common::ValueVector* propertyVector,
        common::sel_t posInPropertyVector) const;
    void delete_(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        DeleteState* deleteState);
    void append(NodeGroup* nodeGroup);

    inline common::column_id_t getNumColumns() const { return columns.size(); }
    inline NodeColumn* getColumn(common::column_id_t columnID) {
        assert(columnID < columns.size());
        return columns[columnID].get();
    }
    inline common::column_id_t getPKColumnID() const { return pkColumnID; }
    inline PrimaryKeyIndex* getPKIndex() const { return pkIndex.get(); }
    inline NodesStatisticsAndDeletedIDs* getNodeStatisticsAndDeletedIDs() const {
        return nodesStatisticsAndDeletedIDs;
    }
    inline common::table_id_t getTableID() const { return tableID; }

    inline void dropColumn(common::column_id_t columnID) {
        columns.erase(columns.begin() + columnID);
    }
    void addColumn(const catalog::Property& property, common::ValueVector* defaultValueVector,
        transaction::Transaction* transaction);

    void prepareCommit();
    void prepareRollback();
    void checkpointInMemory();
    void rollbackInMemory();

private:
    void initializeData(catalog::NodeTableSchema* nodeTableSchema);
    void initializeColumns(catalog::NodeTableSchema* nodeTableSchema);

    void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIds,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIds,
        const std::vector<common::ValueVector*>& outputVectors);

    void insertPK(common::ValueVector* nodeIDVector, common::ValueVector* primaryKeyVector);
    inline uint64_t getNumNodeGroups(transaction::Transaction* transaction) const {
        assert(!columns.empty());
        return columns[0]->getNumNodeGroups(transaction);
    }

private:
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    std::vector<std::unique_ptr<NodeColumn>> columns;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    common::column_id_t pkColumnID;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
    common::table_id_t tableID;
    BufferManager& bufferManager;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
