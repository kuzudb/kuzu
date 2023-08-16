#pragma once

#include "catalog/catalog.h"
#include "storage/copier/node_group.h"
#include "storage/index/hash_index.h"
#include "storage/storage_structure/lists/lists.h"
#include "storage/store/node_column.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"
#include "storage/wal/wal.h"

namespace kuzu {
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
        const std::vector<common::column_id_t>& columnIds,
        const std::vector<common::ValueVector*>& outputVectors);
    void insert(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors,
        const std::unordered_map<common::property_id_t, common::vector_idx_t>&
            propertyIDToVectorIdx);
    void update(transaction::Transaction* transaction, common::property_id_t propertyID,
        common::ValueVector* nodeIDVector, common::ValueVector* propertyVector);
    void update(transaction::Transaction* transaction, common::property_id_t propertyID,
        common::offset_t nodeOffset, common::ValueVector* propertyVector,
        common::sel_t posInPropertyVector);
    void delete_(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        DeleteState* deleteState);
    void append(NodeGroup* nodeGroup);

    std::unordered_set<common::property_id_t> getPropertyIDs() const;

    inline NodeColumn* getPropertyColumn(common::property_id_t propertyIdx) {
        assert(propertyColumns.contains(propertyIdx));
        return propertyColumns.at(propertyIdx).get();
    }
    inline PrimaryKeyIndex* getPKIndex() const { return pkIndex.get(); }
    inline NodesStatisticsAndDeletedIDs* getNodeStatisticsAndDeletedIDs() const {
        return nodesStatisticsAndDeletedIDs;
    }
    inline common::property_id_t getPKPropertyID() const { return pkPropertyID; }
    inline common::table_id_t getTableID() const { return tableID; }

    inline void dropColumn(common::property_id_t propertyID) { propertyColumns.erase(propertyID); }
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
        return propertyColumns.begin()->second->getNumNodeGroups(transaction);
    }

private:
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    // TODO(Guodong): use vector here
    std::map<common::property_id_t, std::unique_ptr<NodeColumn>> propertyColumns;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    common::property_id_t pkPropertyID;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
    common::table_id_t tableID;
    BufferManager& bufferManager;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
