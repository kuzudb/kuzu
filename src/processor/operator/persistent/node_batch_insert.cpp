#include "processor/operator/persistent/node_batch_insert.h"

#include "common/string_format.h"
#include "common/types/types.h"
#include "function/table/scan_functions.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void NodeBatchInsertSharedState::initPKIndex(kuzu::processor::ExecutionContext* context) {
    KU_ASSERT(pkType.getLogicalTypeID() != LogicalTypeID::SERIAL);
    auto indexFName = StorageUtils::getNodeIndexFName(context->clientContext->getVFSUnsafe(),
        wal->getDirectory(), table->getTableID(), FileVersionType::ORIGINAL);
    pkIndex = std::make_unique<PrimaryKeyIndexBuilder>(indexFName, pkType.getPhysicalType(),
        context->clientContext->getVFSUnsafe());
    uint64_t numRows;
    if (readerSharedState != nullptr) {
        KU_ASSERT(distinctSharedState == nullptr);
        auto scanSharedState =
            reinterpret_cast<function::BaseScanSharedState*>(readerSharedState->funcState.get());
        numRows = scanSharedState->numRows;
    } else {
        numRows = distinctSharedState->getFactorizedTable()->getNumTuples();
    }
    pkIndex->bulkReserve(numRows);
    globalIndexBuilder = IndexBuilder(std::make_shared<IndexBuilderSharedState>(pkIndex.get()));
}

void NodeBatchInsertSharedState::appendIncompleteNodeGroup(
    std::unique_ptr<ChunkedNodeGroup> localNodeGroup, std::optional<IndexBuilder>& indexBuilder) {
    std::unique_lock xLck{mtx};
    if (!sharedNodeGroup) {
        sharedNodeGroup = std::move(localNodeGroup);
        return;
    }
    auto numNodesAppended =
        sharedNodeGroup->append(localNodeGroup.get(), 0 /* offsetInNodeGroup */);
    if (sharedNodeGroup->isFull()) {
        auto nodeGroupIdx = getNextNodeGroupIdxWithoutLock();
        auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(table);
        NodeBatchInsert::writeAndResetNodeGroup(nodeGroupIdx, indexBuilder, pkColumnIdx, nodeTable,
            sharedNodeGroup.get());
    }
    if (numNodesAppended < localNodeGroup->getNumRows()) {
        sharedNodeGroup->append(localNodeGroup.get(), numNodesAppended);
    }
}

void NodeBatchInsert::initGlobalStateInternal(ExecutionContext* context) {
    checkIfTableIsEmpty();
    sharedState->logBatchInsertWALRecord();
    auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    if (nodeSharedState->pkType.getLogicalTypeID() != LogicalTypeID::SERIAL) {
        nodeSharedState->initPKIndex(context);
    }
}

void NodeBatchInsert::initLocalStateInternal(ResultSet* resultSet, ExecutionContext*) {
    std::shared_ptr<DataChunkState> state;
    auto nodeInfo = ku_dynamic_cast<BatchInsertInfo*, NodeBatchInsertInfo*>(info.get());
    for (auto& pos : nodeInfo->columnPositions) {
        if (pos.isValid()) {
            state = resultSet->getValueVector(pos)->state;
        }
    }

    auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    localState = std::make_unique<NodeBatchInsertLocalState>();
    auto nodeLocalState =
        ku_dynamic_cast<BatchInsertLocalState*, NodeBatchInsertLocalState*>(localState.get());
    // NOLINTBEGIN(bugprone-unchecked-optional-access)
    if (nodeSharedState->globalIndexBuilder) {
        nodeLocalState->localIndexBuilder = nodeSharedState->globalIndexBuilder.value().clone();
    }
    // NOLINTEND(bugprone-unchecked-optional-access)

    KU_ASSERT(state != nullptr);
    for (auto i = 0u; i < nodeInfo->columnPositions.size(); ++i) {
        auto pos = nodeInfo->columnPositions[i];
        if (pos.isValid()) {
            nodeLocalState->columnVectors.push_back(resultSet->getValueVector(pos).get());
        } else {
            auto& columnType = nodeInfo->columnTypes[i];
            auto nullVector = std::make_shared<ValueVector>(columnType);
            nullVector->setState(state);
            nullVector->setAllNull();
            nodeLocalState->nullColumnVectors.push_back(nullVector);
            nodeLocalState->columnVectors.push_back(nullVector.get());
        }
    }
    nodeLocalState->nodeGroup = NodeGroupFactory::createNodeGroup(ColumnDataFormat::REGULAR,
        nodeInfo->columnTypes, info->compressionEnabled);
    nodeLocalState->columnState = state.get();
}

void NodeBatchInsert::executeInternal(ExecutionContext* context) {
    std::optional<ProducerToken> token;
    auto nodeLocalState =
        ku_dynamic_cast<BatchInsertLocalState*, NodeBatchInsertLocalState*>(localState.get());
    if (nodeLocalState->localIndexBuilder) {
        token = nodeLocalState->localIndexBuilder->getProducerToken();
    }

    while (children[0]->getNextTuple(context)) {
        auto originalSelVector = nodeLocalState->columnState->selVector;
        copyToNodeGroup();
        nodeLocalState->columnState->selVector = std::move(originalSelVector);
    }
    if (nodeLocalState->nodeGroup->getNumRows() > 0) {
        auto nodeSharedState =
            ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(
                sharedState.get());
        nodeSharedState->appendIncompleteNodeGroup(std::move(nodeLocalState->nodeGroup),
            nodeLocalState->localIndexBuilder);
    }
    if (nodeLocalState->localIndexBuilder) {
        KU_ASSERT(token);
        token->quit();
        nodeLocalState->localIndexBuilder->finishedProducing();
    }
}

void NodeBatchInsert::writeAndResetNodeGroup(node_group_idx_t nodeGroupIdx,
    std::optional<IndexBuilder>& indexBuilder, column_id_t pkColumnID, NodeTable* table,
    ChunkedNodeGroup* nodeGroup) {
    nodeGroup->finalize(nodeGroupIdx);
    if (indexBuilder) {
        auto nodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        indexBuilder->insert(nodeGroup->getColumnChunkUnsafe(pkColumnID), nodeOffset,
            nodeGroup->getNumRows());
    }
    table->append(nodeGroup);
    nodeGroup->resetToEmpty();
}

void NodeBatchInsertSharedState::calculateNumTuples() {
    numRows.store(StorageUtils::getStartOffsetOfNodeGroup(getCurNodeGroupIdx()));
    if (sharedNodeGroup) {
        numRows += sharedNodeGroup->getNumRows();
    }
}

void NodeBatchInsert::copyToNodeGroup() {
    auto numAppendedTuples = 0ul;
    auto nodeLocalState =
        ku_dynamic_cast<BatchInsertLocalState*, NodeBatchInsertLocalState*>(localState.get());
    auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(sharedState->table);
    auto numTuplesToAppend = nodeLocalState->columnState->getNumSelectedValues();
    while (numAppendedTuples < numTuplesToAppend) {
        auto numAppendedTuplesInNodeGroup =
            nodeLocalState->nodeGroup->append(nodeLocalState->columnVectors,
                *nodeLocalState->columnState->selVector, numTuplesToAppend - numAppendedTuples);
        numAppendedTuples += numAppendedTuplesInNodeGroup;
        if (nodeLocalState->nodeGroup->isFull()) {
            node_group_idx_t nodeGroupIdx;
            nodeGroupIdx = nodeSharedState->getNextNodeGroupIdx();
            writeAndResetNodeGroup(nodeGroupIdx, nodeLocalState->localIndexBuilder,
                nodeSharedState->pkColumnIdx, nodeTable, nodeLocalState->nodeGroup.get());
        }
        if (numAppendedTuples < numTuplesToAppend) {
            nodeLocalState->columnState->slice((offset_t)numAppendedTuplesInNodeGroup);
        }
    }
}

void NodeBatchInsert::finalize(ExecutionContext* context) {
    auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    nodeSharedState->calculateNumTuples();
    nodeSharedState->updateNumTuplesForTable();
    if (nodeSharedState->sharedNodeGroup) {
        auto nodeGroupIdx = nodeSharedState->getNextNodeGroupIdx();
        auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(nodeSharedState->table);
        NodeBatchInsert::writeAndResetNodeGroup(nodeGroupIdx, nodeSharedState->globalIndexBuilder,
            nodeSharedState->pkColumnIdx, nodeTable, nodeSharedState->sharedNodeGroup.get());
    }
    if (nodeSharedState->globalIndexBuilder) {
        nodeSharedState->globalIndexBuilder->finalize(context);
    }
    auto outputMsg = stringFormat("{} number of tuples has been copied to table: {}.",
        sharedState->getNumRows(), info->tableEntry->getName());
    FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), outputMsg,
        context->clientContext->getMemoryManager());
}
} // namespace processor
} // namespace kuzu
