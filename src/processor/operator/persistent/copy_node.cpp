#include "processor/operator/persistent/copy_node.h"

#include <optional>

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
        auto sharedState =
            reinterpret_cast<function::ScanSharedState*>(readerSharedState->sharedState.get());
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
    if (numNodesAppended < localNodeGroup->getNumRows()) {
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
    std::shared_ptr<DataChunkState> state;
    for (auto& pos : info->columnPositions) {
        if (pos.isValid()) {
            state = resultSet->getValueVector(pos)->state;
        }
    }
    KU_ASSERT(state != nullptr);
    for (auto i = 0; i < info->columnPositions.size(); ++i) {
        auto pos = info->columnPositions[i];
        if (pos.isValid()) {
            columnVectors.push_back(resultSet->getValueVector(pos).get());
        } else {
            auto columnType = sharedState->columnTypes[i].get();
            auto nullVector = std::make_shared<ValueVector>(*columnType);
            nullVector->setState(state);
            nullVector->setAllNull();
            nullColumnVectors.push_back(nullVector);
            columnVectors.push_back(nullVector.get());
        }
    }
    localNodeGroup = NodeGroupFactory::createNodeGroup(
        ColumnDataFormat::REGULAR, sharedState->columnTypes, info->compressionEnabled);
    columnState = state.get();
}

void CopyNode::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        auto originalSelVector = columnState->selVector;
        copyToNodeGroup();
        columnState->selVector = std::move(originalSelVector);
    }
    if (localNodeGroup->getNumRows() > 0) {
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
            nodeGroup->getNumRows() /* startPageIdx */);
    }
    table->append(nodeGroup);
    nodeGroup->resetToEmpty();
}

void CopyNode::populatePKIndex(
    PrimaryKeyIndexBuilder* pkIndex, ColumnChunk* chunk, offset_t startOffset, offset_t numNodes) {
    checkNonNullConstraint(chunk->getNullChunk(), numNodes);
    std::optional<std::string> errorPKValueStr;
    pkIndex->lock();
    try {
        switch (chunk->getDataType()->getPhysicalType()) {
        case PhysicalTypeID::INT64: {
            auto numAppendedNodes = appendToPKIndex<int64_t>(pkIndex, chunk, startOffset, numNodes);
            if (numAppendedNodes < numNodes) {
                // TODO(bmwinger): This should be tested where there are multiple node groups
                errorPKValueStr = std::to_string(chunk->getValue<int64_t>(numAppendedNodes));
            }
        } break;
        case PhysicalTypeID::STRING: {
            auto numAppendedNodes =
                appendToPKIndex<std::string>(pkIndex, chunk, startOffset, numNodes);
            if (numAppendedNodes < numNodes) {
                // TODO(bmwinger): This should be tested where there are multiple node groups
                errorPKValueStr =
                    static_cast<StringColumnChunk*>(chunk)->getValue<std::string>(numAppendedNodes);
            }
        } break;
        default: {
            throw CopyException(ExceptionMessage::invalidPKType(chunk->getDataType()->toString()));
        }
        }
    } catch (Exception& e) {
        pkIndex->unlock();
        throw;
    }
    pkIndex->unlock();
    if (errorPKValueStr) {
        throw CopyException(ExceptionMessage::existedPKException(*errorPKValueStr));
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
        numNodes += sharedState->sharedNodeGroup->getNumRows();
        auto nodeGroupIdx = sharedState->getNextNodeGroupIdx();
        writeAndResetNodeGroup(nodeGroupIdx, sharedState->pkIndex.get(), sharedState->pkColumnIdx,
            sharedState->table, sharedState->sharedNodeGroup.get());
    }
    if (sharedState->pkIndex) {
        sharedState->pkIndex->flush();
    }
    sharedState->table->getNodeStatisticsAndDeletedIDs()->setNumTuplesForTable(
        sharedState->table->getTableID(), numNodes);
    for (auto relTable : info->fwdRelTables) {
        relTable->resizeColumns(context->clientContext->getActiveTransaction(),
            RelDataDirection::FWD, sharedState->getCurNodeGroupIdx());
    }
    for (auto relTable : info->bwdRelTables) {
        relTable->resizeColumns(context->clientContext->getActiveTransaction(),
            RelDataDirection::BWD, sharedState->getCurNodeGroupIdx());
    }
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
uint64_t CopyNode::appendToPKIndex<std::string>(
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
