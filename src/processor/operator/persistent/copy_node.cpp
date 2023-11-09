#include "processor/operator/persistent/copy_node.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "function/table_functions/scan_functions.h"
#include "processor/result/factorized_table.h"
#include "storage/store/string_column_chunk.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void CopyNodeSharedState::init() {
    if (pkType->getLogicalTypeID() != LogicalTypeID::SERIAL) {
        auto indexFName = StorageUtils::getNodeIndexFName(
            wal->getDirectory(), table->getTableID(), FileVersionType::ORIGINAL);
        pkIndex = std::make_unique<PrimaryKeyIndexBuilder>(indexFName, *pkType);
        auto sharedState = reinterpret_cast<function::ScanSharedTableFuncState*>(
            readerSharedState->sharedState.get());
        pkIndex->bulkReserve(sharedState->numRows);
    }
    wal->logCopyTableRecord(table->getTableID(), TableType::NODE);
    wal->flushAllPages();
}

void CopyNodeSharedState::appendLocalNodeGroup(std::unique_ptr<NodeGroup> localNodeGroup) {
    std::unique_lock xLck{mtx};
    if (!sharedNodeGroup) {
        sharedNodeGroup = std::move(localNodeGroup);
        return;
    }
    auto numNodesAppended =
        sharedNodeGroup->append(localNodeGroup.get(), 0 /* offsetInNodeGroup */);
    if (sharedNodeGroup->isFull()) {
        auto nodeGroupIdx = getNextNodeGroupIdxWithoutLock();
        CopyNode::writeAndResetNodeGroup(
            nodeGroupIdx, pkIndex.get(), pkColumnIdx, table, sharedNodeGroup.get());
    }
    if (numNodesAppended < localNodeGroup->getNumNodes()) {
        sharedNodeGroup->append(localNodeGroup.get(), numNodesAppended);
    }
}

static bool isEmptyTable(NodeTable* nodeTable) {
    auto nodesStatistics = nodeTable->getNodeStatisticsAndDeletedIDs();
    return nodesStatistics->getNodeStatisticsAndDeletedIDs(nodeTable->getTableID())
               ->getNumTuples() == 0;
}

void CopyNode::initGlobalStateInternal(ExecutionContext* /*context*/) {
    if (!isEmptyTable(info->table)) {
        throw CopyException(ExceptionMessage::notAllowCopyOnNonEmptyTableException());
    }
    sharedState->init();
}

void CopyNode::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    for (auto& pos : info->columnPositions) {
        columnVectors.push_back(resultSet->getValueVector(pos).get());
    }
    localNodeGroup = NodeGroupFactory::createNodeGroup(
        ColumnDataFormat::REGULAR, sharedState->columnTypes, info->compressionEnabled);
    columnState = resultSet->getDataChunk(info->columnPositions[0].dataChunkPos)->state.get();
}

void CopyNode::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        auto originalSelVector = columnState->selVector;
        copyToNodeGroup();
        columnState->selVector = std::move(originalSelVector);
    }
    if (localNodeGroup->getNumNodes() > 0) {
        sharedState->appendLocalNodeGroup(std::move(localNodeGroup));
    }
}

void CopyNode::writeAndResetNodeGroup(node_group_idx_t nodeGroupIdx,
    PrimaryKeyIndexBuilder* pkIndex, column_id_t pkColumnID, NodeTable* table,
    NodeGroup* nodeGroup) {
    nodeGroup->finalize(nodeGroupIdx);
    auto startOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    if (pkIndex) {
        populatePKIndex(pkIndex, nodeGroup->getColumnChunk(pkColumnID), startOffset,
            nodeGroup->getNumNodes() /* startPageIdx */);
    }
    table->append(nodeGroup);
    nodeGroup->resetToEmpty();
}

void CopyNode::populatePKIndex(
    PrimaryKeyIndexBuilder* pkIndex, ColumnChunk* chunk, offset_t startOffset, offset_t numNodes) {
    checkNonNullConstraint(chunk->getNullChunk(), numNodes);
    std::string errorPKValueStr;
    pkIndex->lock();
    try {
        switch (chunk->getDataType().getPhysicalType()) {
        case PhysicalTypeID::INT64: {
            auto numAppendedNodes = appendToPKIndex<int64_t>(pkIndex, chunk, startOffset, numNodes);
            if (numAppendedNodes < numNodes) {
                errorPKValueStr =
                    std::to_string(chunk->getValue<int64_t>(startOffset + numAppendedNodes));
            }
        } break;
        case PhysicalTypeID::STRING: {
            auto numAppendedNodes =
                appendToPKIndex<ku_string_t>(pkIndex, chunk, startOffset, numNodes);
            if (numAppendedNodes < numNodes) {
                errorPKValueStr =
                    chunk->getValue<ku_string_t>(startOffset + numAppendedNodes).getAsString();
            }
        } break;
        default: {
            throw CopyException(ExceptionMessage::invalidPKType(chunk->getDataType().toString()));
        }
        }
    } catch (Exception& e) {
        pkIndex->unlock();
        throw;
    }
    pkIndex->unlock();
    if (!errorPKValueStr.empty()) {
        throw CopyException(ExceptionMessage::existedPKException(errorPKValueStr));
    }
}

void CopyNode::checkNonNullConstraint(NullColumnChunk* nullChunk, offset_t numNodes) {
    for (auto posInChunk = 0u; posInChunk < numNodes; posInChunk++) {
        if (nullChunk->isNull(posInChunk)) {
            throw CopyException(ExceptionMessage::nullPKException());
        }
    }
}

void CopyNode::finalize(ExecutionContext* context) {
    uint64_t numNodes = StorageUtils::getStartOffsetOfNodeGroup(sharedState->getCurNodeGroupIdx());
    if (sharedState->sharedNodeGroup) {
        numNodes += sharedState->sharedNodeGroup->getNumNodes();
        auto nodeGroupIdx = sharedState->getNextNodeGroupIdx();
        writeAndResetNodeGroup(nodeGroupIdx, sharedState->pkIndex.get(), sharedState->pkColumnIdx,
            sharedState->table, sharedState->sharedNodeGroup.get());
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

template<>
uint64_t CopyNode::appendToPKIndex<int64_t>(
    PrimaryKeyIndexBuilder* pkIndex, ColumnChunk* chunk, offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<int64_t>(i);
        if (!pkIndex->append(value, offset)) {
            return i;
        }
    }
    return numValues;
}

template<>
uint64_t CopyNode::appendToPKIndex<ku_string_t>(
    PrimaryKeyIndexBuilder* pkIndex, ColumnChunk* chunk, offset_t startOffset, uint64_t numValues) {
    auto stringColumnChunk = (StringColumnChunk*)chunk;
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = stringColumnChunk->getValue<std::string>(i);
        if (!pkIndex->append(value.c_str(), offset)) {
            return i;
        }
    }
    return numValues;
}

void CopyNode::copyToNodeGroup() {
    auto numAppendedTuples = 0ul;
    auto numTuplesToAppend = columnState->getNumSelectedValues();
    while (numAppendedTuples < numTuplesToAppend) {
        auto numAppendedTuplesInNodeGroup = localNodeGroup->append(
            columnVectors, columnState, numTuplesToAppend - numAppendedTuples);
        numAppendedTuples += numAppendedTuplesInNodeGroup;
        if (localNodeGroup->isFull()) {
            node_group_idx_t nodeGroupIdx;
            nodeGroupIdx = sharedState->getNextNodeGroupIdx();
            writeAndResetNodeGroup(nodeGroupIdx, sharedState->pkIndex.get(),
                sharedState->pkColumnIdx, sharedState->table, localNodeGroup.get());
        }
        if (numAppendedTuples < numTuplesToAppend) {
            columnState->slice((offset_t)numAppendedTuplesInNodeGroup);
        }
    }
}

} // namespace processor
} // namespace kuzu
