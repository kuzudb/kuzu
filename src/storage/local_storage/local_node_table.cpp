#include "storage/local_storage/local_node_table.h"

#include "common/cast.h"
#include "storage/storage_utils.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void LocalNodeNG::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    KU_ASSERT(columnIDs.size() == outputVectors.size());
    for (auto i = 0u; i < columnIDs.size(); i++) {
        auto columnID = columnIDs[i];
        for (auto pos = 0u; pos < nodeIDVector->state->selVector->selectedSize; pos++) {
            auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[pos];
            auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
            auto posInOutputVector = outputVectors[i]->state->selVector->selectedPositions[pos];
            lookup(nodeOffset, columnID, outputVectors[i], posInOutputVector);
        }
    }
}

void LocalNodeNG::lookup(offset_t nodeOffset, column_id_t columnID, ValueVector* outputVector,
    sel_t posInOutputVector) {
    if (deleteInfo.containsOffset(nodeOffset)) {
        // Node has been deleted.
        return;
    }
    if (insertChunks.read(nodeOffset, columnID, outputVector, posInOutputVector)) {
        // Node has been newly inserted.
        return;
    }
    updateChunks[columnID].read(nodeOffset, 0 /*columnID*/, outputVector, posInOutputVector);
}

bool LocalNodeNG::insert(std::vector<common::ValueVector*> nodeIDVectors,
    std::vector<common::ValueVector*> propertyVectors) {
    KU_ASSERT(nodeIDVectors.size() == 1);
    auto nodeIDVector = nodeIDVectors[0];
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
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
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
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
    KU_ASSERT(nodeIDVector->state->selVector->selectedSize == 1);
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
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

void LocalNodeTableData::scan(ValueVector* nodeIDVector, const std::vector<column_id_t>& columnIDs,
    const std::vector<ValueVector*>& outputVectors) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    if (!nodeGroups.contains(nodeGroupIdx)) {
        return;
    }
    auto localNodeGroup =
        ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(nodeGroups.at(nodeGroupIdx).get());
    KU_ASSERT(localNodeGroup);
    localNodeGroup->scan(nodeIDVector, columnIDs, outputVectors);
}

void LocalNodeTableData::lookup(ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[i];
        auto nodeOffset = nodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
        auto localNodeGroup =
            ku_dynamic_cast<LocalNodeGroup*, LocalNodeNG*>(getOrCreateLocalNodeGroup(nodeIDVector));
        KU_ASSERT(localNodeGroup);
        for (auto columnIdx = 0u; columnIdx < columnIDs.size(); columnIdx++) {
            auto columnID = columnIDs[columnIdx];
            auto outputVector = outputVectors[columnIdx];
            auto outputVectorPos = outputVector->state->selVector->selectedPositions[i];
            localNodeGroup->lookup(nodeOffset, columnID, outputVector, outputVectorPos);
        }
    }
}

LocalNodeGroup* LocalNodeTableData::getOrCreateLocalNodeGroup(common::ValueVector* nodeIDVector) {
    auto nodeIDPos = nodeIDVector->state->selVector->selectedPositions[0];
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

void LocalNodeTable::scan(TableReadState& state) {
    auto localTableData =
        ku_dynamic_cast<LocalTableData*, LocalNodeTableData*>(localTableDataCollection[0].get());
    localTableData->scan(const_cast<ValueVector*>(&state.nodeIDVector), state.columnIDs,
        state.outputVectors);
}

void LocalNodeTable::lookup(TableReadState& state) {
    auto localTableData =
        ku_dynamic_cast<LocalTableData*, LocalNodeTableData*>(localTableDataCollection[0].get());
    localTableData->lookup(const_cast<ValueVector*>(&state.nodeIDVector), state.columnIDs,
        state.outputVectors);
}

} // namespace storage
} // namespace kuzu
