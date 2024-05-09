#include "storage/local_storage/local_node_table.h"

#include "common/cast.h"
#include "storage/storage_utils.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void LocalNodeNG::scan(const ValueVector& nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() == outputVectors.size());
    for (auto pos = 0u; pos < nodeIDVector.state->getSelVector().getSelSize(); pos++) {
        lookup(nodeIDVector, pos, columnIDs, outputVectors);
    }
}

void LocalNodeNG::lookup(const common::ValueVector& nodeIDVector,
    common::offset_t offsetInVectorToLookup, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    auto nodeIDPos = nodeIDVector.state->getSelVector()[offsetInVectorToLookup];
    auto nodeOffset = nodeIDVector.getValue<nodeID_t>(nodeIDPos).offset;
    if (deleteInfo.containsOffset(nodeOffset)) {
        // Node has been deleted.
        return;
    }
    if (insertChunks.read(nodeOffset, columnIDs, outputVectors, offsetInVectorToLookup)) {
        // Node has been newly inserted.
        return;
    }
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto posInOutputVector = outputVectors[i]->state->getSelVector()[offsetInVectorToLookup];
        if (columnIDs[i] == INVALID_COLUMN_ID) {
            outputVectors[i]->setNull(posInOutputVector, true);
            continue;
        }
        getUpdateChunks(columnIDs[i])
            .read(nodeOffset, 0 /*columnID*/, outputVectors[i], posInOutputVector);
    }
}

bool LocalNodeNG::insert(std::vector<common::ValueVector*> nodeIDVectors,
    std::vector<common::ValueVector*> propertyVectors) {
    KU_ASSERT(nodeIDVectors.size() == 1);
    auto nodeIDVector = nodeIDVectors[0];
    KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1);
    auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return false;
    }
    // The nodeOffset here should be the offset within the node group.
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE);
    insertChunks.append(nodeOffset, propertyVectors);
    return true;
}

bool LocalNodeNG::update(std::vector<common::ValueVector*> nodeIDVectors,
    common::column_id_t columnID, common::ValueVector* propertyVector) {
    KU_ASSERT(nodeIDVectors.size() == 1);
    auto nodeIDVector = nodeIDVectors[0];
    KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1);
    auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return false;
    }
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE && columnID < updateChunks.size());
    // Check if the node is newly inserted or in persistent storage.
    if (insertChunks.hasOffset(nodeOffset)) {
        insertChunks.update(nodeOffset, columnID, propertyVector);
    } else {
        updateChunks[columnID].append(nodeOffset, {propertyVector});
    }
    return true;
}

bool LocalNodeNG::delete_(common::ValueVector* nodeIDVector, common::ValueVector* /*extraVector*/) {
    KU_ASSERT(nodeIDVector->state->getSelVector().getSelSize() == 1);
    auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
    if (nodeIDVector->isNull(nodeIDPos)) {
        return false;
    }
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset - nodeGroupStartOffset;
    KU_ASSERT(nodeOffset < StorageConstants::NODE_GROUP_SIZE);
    // Check if the node is newly inserted or in persistent storage.
    if (insertChunks.hasOffset(nodeOffset)) {
        insertChunks.remove(nodeOffset);
    } else {
        for (auto i = 0u; i < updateChunks.size(); i++) {
            updateChunks[i].remove(nodeOffset);
        }
        deleteInfo.deleteOffset(nodeOffset);
    }
    return true;
}

LocalNodeGroup* LocalNodeTableData::getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->getSelVector()[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        auto nodeGroupStartOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        nodeGroups[nodeGroupIdx] = std::make_unique<LocalNodeNG>(nodeGroupStartOffset, dataTypes);
    }
    return nodeGroups.at(nodeGroupIdx).get();
}

LocalNodeTable::LocalNodeTable(Table& table) : LocalTable{table} {
    auto& nodeTable = ku_dynamic_cast<Table&, NodeTable&>(table);
    std::vector<LogicalType> types;
    types.reserve(nodeTable.getNumColumns());
    for (auto i = 0u; i < nodeTable.getNumColumns(); i++) {
        types.push_back(nodeTable.getColumn(i)->getDataType());
    }
    localTableDataCollection.push_back(std::make_unique<LocalNodeTableData>(types));
}

bool LocalNodeTable::insert(TableInsertState& state) {
    auto& insertState = ku_dynamic_cast<TableInsertState&, NodeTableInsertState&>(state);
    return localTableDataCollection[0]->insert({&insertState.nodeIDVector},
        insertState.propertyVectors);
}

bool LocalNodeTable::update(TableUpdateState& state) {
    auto& updateState = ku_dynamic_cast<TableUpdateState&, NodeTableUpdateState&>(state);
    return localTableDataCollection[0]->update(
        {const_cast<ValueVector*>(&updateState.nodeIDVector)}, updateState.columnID,
        const_cast<ValueVector*>(&updateState.propertyVector));
}

bool LocalNodeTable::delete_(TableDeleteState& deleteState) {
    auto& deleteState_ = ku_dynamic_cast<TableDeleteState&, NodeTableDeleteState&>(deleteState);
    return localTableDataCollection[0]->delete_(
        const_cast<ValueVector*>(&deleteState_.nodeIDVector), nullptr);
}

} // namespace storage
} // namespace kuzu
