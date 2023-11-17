#include "processor/operator/persistent/copy_rdf_resource.h"

#include "common/exception/copy.h"
#include "common/string_format.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static bool isEmptyTable(NodeTable* nodeTable) {
    auto nodesStatistics = nodeTable->getNodeStatisticsAndDeletedIDs();
    return nodesStatistics->getNodeStatisticsAndDeletedIDs(nodeTable->getTableID())
               ->getNumTuples() == 0;
}

void CopyRdfResource::initGlobalStateInternal(ExecutionContext* /*context*/) {
    // LCOV_EXCL_START
    if (!isEmptyTable(info->table)) {
        throw CopyException("aa");
    }
    // LCOV_EXCL_STOP
    sharedState->init();
}

void CopyRdfResource::initLocalStateInternal(
    ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) {
    KU_ASSERT(info->columnPositions.size() == 1);
    vector = resultSet->getValueVector(info->columnPositions[0]).get();
    columnState = vector->state.get();
    localNodeGroup = NodeGroupFactory::createNodeGroup(
        ColumnDataFormat::REGULAR, sharedState->columnTypes, info->compressionEnabled);
}

static void writeNodeGroup(node_group_idx_t nodeGroupIdx, NodeTable* table, NodeGroup* nodeGroup) {
    nodeGroup->finalize(nodeGroupIdx);
    table->append(nodeGroup);
    nodeGroup->resetToEmpty();
}

void CopyRdfResource::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        auto originalSelVector = columnState->selVector;
        insertIndex(vector);
        copyToNodeGroup(vector);
        columnState->selVector = std::move(originalSelVector);
    }
    if (localNodeGroup->getNumRows() > 0) {
        sharedState->appendLocalNodeGroup(std::move(localNodeGroup));
    }
}

void CopyRdfResource::finalize(ExecutionContext* context) {
    uint64_t numNodes = StorageUtils::getStartOffsetOfNodeGroup(sharedState->getCurNodeGroupIdx());
    if (sharedState->sharedNodeGroup) {
        numNodes += sharedState->sharedNodeGroup->getNumRows();
        auto nodeGroupIdx = sharedState->getNextNodeGroupIdx();
        writeNodeGroup(nodeGroupIdx, sharedState->table, sharedState->sharedNodeGroup.get());
    }
    if (sharedState->pkIndex) {
        sharedState->pkIndex->flush();
    }
    sharedState->table->getNodeStatisticsAndDeletedIDs()->setNumTuplesForTable(
        sharedState->table->getTableID(), numNodes);
    auto outputMsg = stringFormat(
        "{} number of tuples has been copied to table: {}.", numNodes, info->tableName.c_str());
    FactorizedTableUtils::appendStringToTable(
        sharedState->fTable.get(), outputMsg, context->memoryManager);
}

void CopyRdfResource::insertIndex(ValueVector* vector) {
    auto selVector = std::make_unique<SelectionVector>(common::DEFAULT_VECTOR_CAPACITY);
    selVector->resetSelectorToValuePosBuffer();
    common::sel_t nextPos = 0;
    common::offset_t result;
    auto offset = StorageUtils::getStartOffsetOfNodeGroup(sharedState->getCurNodeGroupIdx()) +
                  localNodeGroup->getNumRows();
    for (auto i = 0u; i < vector->state->getNumSelectedValues(); i++) {
        auto uriStr = vector->getValue<common::ku_string_t>(i).getAsString();
        if (!sharedState->pkIndex->lookup(uriStr.c_str(), result)) {
            sharedState->pkIndex->append(uriStr.c_str(), offset++);
            selVector->selectedPositions[nextPos++] = i;
        }
    }
    selVector->selectedSize = nextPos;
    vector->state->selVector = std::move(selVector);
}

void CopyRdfResource::copyToNodeGroup(ValueVector* vector) {
    auto numAppendedTuples = 0ul;
    auto numTuplesToAppend = columnState->getNumSelectedValues();
    while (numAppendedTuples < numTuplesToAppend) {
        auto numAppendedTuplesInNodeGroup = localNodeGroup->append(
            std::vector<ValueVector*>{vector}, columnState, numTuplesToAppend - numAppendedTuples);
        numAppendedTuples += numAppendedTuplesInNodeGroup;
        if (localNodeGroup->isFull()) {
            node_group_idx_t nodeGroupIdx;
            nodeGroupIdx = sharedState->getNextNodeGroupIdx();
            writeNodeGroup(nodeGroupIdx, sharedState->table, localNodeGroup.get());
        }
        if (numAppendedTuples < numTuplesToAppend) {
            columnState->slice((offset_t)numAppendedTuplesInNodeGroup);
        }
    }
}

} // namespace processor
} // namespace kuzu
