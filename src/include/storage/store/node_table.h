#pragma once

#include "catalog/catalog.h"
#include "storage/index/hash_index.h"
#include "storage/storage_structure/lists/lists.h"
#include "storage/store/nodes_statistics_and_deleted_ids.h"
#include "storage/wal/wal.h"

namespace kuzu {
namespace storage {

class NodeTable {

public:
    NodeTable(NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs,
        BufferManager& bufferManager, WAL* wal, catalog::NodeTableSchema* nodeTableSchema);

    void initializeData(catalog::NodeTableSchema* nodeTableSchema);
    static std::unordered_map<common::property_id_t, std::unique_ptr<Column>> initializeColumns(
        WAL* wal, BufferManager* bm, catalog::NodeTableSchema* nodeTableSchema);

    inline common::offset_t getMaxNodeOffset(transaction::Transaction* trx) const {
        return nodesStatisticsAndDeletedIDs->getMaxNodeOffset(trx, tableID);
    }
    inline void setSelVectorForDeletedOffsets(
        transaction::Transaction* trx, std::shared_ptr<common::ValueVector>& vector) const {
        assert(vector->isSequential());
        nodesStatisticsAndDeletedIDs->setDeletedNodeOffsetsForMorsel(trx, vector, tableID);
    }

    void scan(transaction::Transaction* transaction, common::ValueVector* inputIDVector,
        const std::vector<uint32_t>& columnIdxes, std::vector<common::ValueVector*> outputVectors);

    inline Column* getPropertyColumn(common::property_id_t propertyIdx) {
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
        propertyColumns.emplace(property.propertyID,
            ColumnFactory::getColumn(StorageUtils::getNodePropertyColumnStructureIDAndFName(
                                         wal->getDirectory(), property),
                property.dataType, &bufferManager, wal));
    }
    common::offset_t addNodeAndResetProperties();
    common::offset_t addNodeAndResetPropertiesWithPK(common::ValueVector* primaryKeyVector);
    void deleteNodes(common::ValueVector* nodeIDVector, common::ValueVector* primaryKeyVector);

    void prepareCommit();
    void prepareRollback();
    inline void checkpointInMemory() { pkIndex->checkpointInMemory(); }
    inline void rollback() { pkIndex->rollback(); }

private:
    void deleteNode(
        common::offset_t nodeOffset, common::ValueVector* primaryKeyVector, uint32_t pos) const;

private:
    NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs;
    std::unordered_map<common::property_id_t, std::unique_ptr<Column>> propertyColumns;
    std::unique_ptr<PrimaryKeyIndex> pkIndex;
    common::table_id_t tableID;
    BufferManager& bufferManager;
    WAL* wal;
};

} // namespace storage
} // namespace kuzu
