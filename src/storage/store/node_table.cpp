#include "storage/store/node_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

NodeTable::NodeTable(NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs,
    BufferManager& bufferManager, WAL* wal, NodeTableSchema* nodeTableSchema)
    : nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs}, tableID{nodeTableSchema->tableID},
      bufferManager{bufferManager}, wal{wal} {
    initializeData(nodeTableSchema);
}

std::unordered_map<common::property_id_t, std::unique_ptr<Column>> NodeTable::initializeColumns(
    WAL* wal, kuzu::storage::BufferManager* bm, NodeTableSchema* nodeTableSchema) {
    std::unordered_map<common::property_id_t, std::unique_ptr<Column>> propertyColumns;
    for (auto& property : nodeTableSchema->getAllNodeProperties()) {
        propertyColumns[property.propertyID] = ColumnFactory::getColumn(
            StorageUtils::getNodePropertyColumnStructureIDAndFName(wal->getDirectory(), property),
            property.dataType, bm, wal);
    }
    return propertyColumns;
}

void NodeTable::initializeData(NodeTableSchema* nodeTableSchema) {
    propertyColumns = initializeColumns(wal, &bufferManager, nodeTableSchema);
    if (nodeTableSchema->getPrimaryKey().dataType.getLogicalTypeID() != LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndex>(
            StorageUtils::getNodeIndexIDAndFName(wal->getDirectory(), tableID),
            nodeTableSchema->getPrimaryKey().dataType, bufferManager, wal);
    }
}

void NodeTable::scan(transaction::Transaction* transaction, ValueVector* inputIDVector,
    const std::vector<uint32_t>& columnIds, std::vector<ValueVector*> outputVectors) {
    assert(columnIds.size() == outputVectors.size());
    for (auto i = 0u; i < columnIds.size(); i++) {
        if (columnIds[i] == UINT32_MAX) {
            outputVectors[i]->setAllNull();
        } else {
            propertyColumns.at(columnIds[i])->read(transaction, inputIDVector, outputVectors[i]);
        }
    }
}

offset_t NodeTable::addNodeAndResetProperties() {
    auto nodeOffset = nodesStatisticsAndDeletedIDs->addNode(tableID);
    for (auto& [_, column] : propertyColumns) {
        if (column->dataType.getLogicalTypeID() != LogicalTypeID::SERIAL) {
            column->setNull(nodeOffset);
        }
    }
    return nodeOffset;
}

offset_t NodeTable::addNodeAndResetPropertiesWithPK(common::ValueVector* primaryKeyVector) {
    auto nodeOffset = addNodeAndResetProperties();
    assert(primaryKeyVector->state->selVector->selectedSize == 1);
    auto pkValPos = primaryKeyVector->state->selVector->selectedPositions[0];
    if (primaryKeyVector->isNull(pkValPos)) {
        throw RuntimeException("Null is not allowed as a primary key value.");
    }
    if (!pkIndex->insert(primaryKeyVector, pkValPos, nodeOffset)) {
        std::string pkStr = primaryKeyVector->dataType.getLogicalTypeID() == LogicalTypeID::INT64 ?
                                std::to_string(primaryKeyVector->getValue<int64_t>(pkValPos)) :
                                primaryKeyVector->getValue<ku_string_t>(pkValPos).getAsString();
        throw RuntimeException(Exception::getExistedPKExceptionMsg(pkStr));
    }
    return nodeOffset;
}

void NodeTable::deleteNodes(ValueVector* nodeIDVector, ValueVector* primaryKeyVector) {
    assert(nodeIDVector->state == primaryKeyVector->state && nodeIDVector->hasNoNullsGuarantee() &&
           primaryKeyVector->hasNoNullsGuarantee());
    if (nodeIDVector->state->selVector->selectedSize == 1) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[0];
        deleteNode(nodeIDVector->readNodeOffset(pos), primaryKeyVector, pos);
    } else {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            deleteNode(nodeIDVector->readNodeOffset(pos), primaryKeyVector, pos);
        }
    }
}

void NodeTable::prepareCommit() {
    if (pkIndex) {
        pkIndex->prepareCommit();
    }
}

void NodeTable::prepareRollback() {
    if (pkIndex) {
        pkIndex->prepareRollback();
    }
}

void NodeTable::deleteNode(offset_t nodeOffset, ValueVector* primaryKeyVector, uint32_t pos) const {
    nodesStatisticsAndDeletedIDs->deleteNode(tableID, nodeOffset);
    if (pkIndex) {
        pkIndex->deleteKey(primaryKeyVector, pos);
    }
}

} // namespace storage
} // namespace kuzu
