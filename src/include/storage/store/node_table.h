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
    NodeTable(BMFileHandle* dataFH, BMFileHandle* metadataFH,
        NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs, BufferManager& bufferManager,
        WAL* wal, catalog::NodeTableSchema* nodeTableSchema);

    void initializePKIndex(catalog::NodeTableSchema* nodeTableSchema);

    inline common::offset_t getMaxNodeOffset(transaction::Transaction* transaction) const {
        return nodesStatisticsAndDeletedIDs->getMaxNodeOffset(transaction, tableID);
    }
    inline uint64_t getNumNodeGroups(transaction::Transaction* transaction) const {
        return propertyColumns.begin()->second->getNumNodeGroups(transaction);
    }
    inline void setSelVectorForDeletedOffsets(
        transaction::Transaction* trx, std::shared_ptr<common::ValueVector>& vector) const {
        assert(vector->isSequential());
        nodesStatisticsAndDeletedIDs->setDeletedNodeOffsetsForMorsel(trx, vector, tableID);
    }
    inline BMFileHandle* getDataFH() const { return dataFH; }

    void read(transaction::Transaction* transaction, common::ValueVector* inputIDVector,
        const std::vector<common::column_id_t>& columnIds,
        const std::vector<common::ValueVector*>& outputVectors);
    void write(common::property_id_t propertyID, common::ValueVector* nodeIDVector,
        common::ValueVector* vectorToWriteFrom);

    common::offset_t addNode(transaction::Transaction* transaction);

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
    inline common::table_id_t getTableID() const { return tableID; }

    inline void removeProperty(common::property_id_t propertyID) {
        propertyColumns.erase(propertyID);
    }
    inline void addProperty(const catalog::Property& property) {
        propertyColumns.emplace(property.getPropertyID(),
            ColumnFactory::getColumn(StorageUtils::getNodePropertyColumnStructureIDAndFName(
                                         wal->getDirectory(), property),
                *property.getDataType(), &bufferManager, wal));
    }
    void deleteNode(
        common::offset_t nodeOffset, common::ValueVector* primaryKeyVector, uint32_t pos) const;
    void setPropertiesToNull(common::offset_t offset);
    void insertPK(common::offset_t offset, common::ValueVector* primaryKeyVector);

    void prepareCommit();
    void prepareRollback();
    void checkpointInMemory();
    void rollbackInMemory();

private:
    void initializeData(catalog::NodeTableSchema* nodeTableSchema);
    void initializeColumns(catalog::NodeTableSchema* nodeTableSchema);

    void scan(transaction::Transaction* transaction, common::ValueVector* inputIDVector,
        const std::vector<common::column_id_t>& columnIds,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(transaction::Transaction* transaction, common::ValueVector* inputIDVector,
        const std::vector<common::column_id_t>& columnIds,
        const std::vector<common::ValueVector*>& outputVectors);

private:
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    std::map<common::property_id_t, std::unique_ptr<NodeColumn>> propertyColumns;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
    common::table_id_t tableID;
    BufferManager& bufferManager;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
