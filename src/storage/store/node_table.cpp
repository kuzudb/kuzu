#include "storage/store/node_table.h"

namespace kuzu {
namespace storage {

NodeTable::NodeTable(NodesStatisticsAndDeletedIDs* nodesStatisticsAndDeletedIDs,
    BufferManager& bufferManager, bool isInMemory, WAL* wal, NodeTableSchema* nodeTableSchema)
    : nodesStatisticsAndDeletedIDs{nodesStatisticsAndDeletedIDs}, tableID{nodeTableSchema->tableID},
      isInMemory{isInMemory} {
    initializeData(nodeTableSchema, bufferManager, wal);
}

void NodeTable::initializeData(
    NodeTableSchema* nodeTableSchema, BufferManager& bufferManager, WAL* wal) {
    propertyColumns.resize(nodeTableSchema->getAllNodeProperties().size());
    for (auto i = 0u; i < nodeTableSchema->getAllNodeProperties().size(); i++) {
        auto property = nodeTableSchema->getAllNodeProperties()[i];
        propertyColumns[i] = ColumnFactory::getColumn(
            StorageUtils::getStructuredNodePropertyColumnStructureIDAndFName(
                wal->getDirectory(), property),
            property.dataType, bufferManager, isInMemory, wal);
    }
    pkIndex = make_unique<PrimaryKeyIndex>(
        StorageUtils::getNodeIndexIDAndFName(wal->getDirectory(), tableID),
        nodeTableSchema->getPrimaryKey().dataType, bufferManager, wal);
}

void NodeTable::scan(Transaction* transaction, const shared_ptr<ValueVector>& inputIDVector,
    const vector<uint32_t>& columnIds, vector<shared_ptr<ValueVector>> outputVectors) {
    assert(columnIds.size() == outputVectors.size());
    for (auto i = 0u; i < columnIds.size(); i++) {
        if (columnIds[i] == UINT32_MAX) {
            outputVectors[i]->setAllNull();
        } else {
            propertyColumns[columnIds[i]]->read(transaction, inputIDVector, outputVectors[i]);
        }
    }
}

node_offset_t NodeTable::addNodeAndResetProperties(ValueVector* primaryKeyVector) {
    auto nodeOffset = nodesStatisticsAndDeletedIDs->addNode(tableID);
    assert(primaryKeyVector->state->selVector->selectedSize == 1);
    auto pkValPos = primaryKeyVector->state->selVector->selectedPositions[0];
    if (primaryKeyVector->isNull(pkValPos)) {
        throw RuntimeException("Null is not allowed as a primary key value.");
    }
    if (!pkIndex->insert(primaryKeyVector, pkValPos, nodeOffset)) {
        string pkStr = primaryKeyVector->dataType.typeID == INT64 ?
                           to_string(primaryKeyVector->getValue<int64_t>(pkValPos)) :
                           primaryKeyVector->getValue<ku_string_t>(pkValPos).getAsString();
        throw RuntimeException(Exception::getExistedPKExceptionMsg(pkStr));
    }
    for (auto& column : propertyColumns) {
        column->setNodeOffsetToNull(nodeOffset);
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

void NodeTable::deleteNode(
    node_offset_t nodeOffset, ValueVector* primaryKeyVector, uint32_t pos) const {
    nodesStatisticsAndDeletedIDs->deleteNode(tableID, nodeOffset);
    pkIndex->deleteKey(primaryKeyVector, pos);
}

void NodeTable::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    pkIndex->prepareCommitOrRollbackIfNecessary(isCommit);
}

} // namespace storage
} // namespace kuzu
