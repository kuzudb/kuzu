#include "processor/operator/persistent/node_batch_insert.h"

#include "common/cast.h"
#include "common/constants.h"
#include "common/string_format.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "function/table/scan_functions.h"
#include "processor/execution_context.h"
#include "processor/operator/persistent/index_builder.h"
#include "processor/result/factorized_table.h"
#include "processor/result/factorized_table_util.h"
#include "storage/local_storage/local_node_table.h"
#include "storage/store/chunked_node_group.h"
#include "storage/store/node_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void NodeBatchInsertSharedState::initPKIndex(kuzu::processor::ExecutionContext* /*context*/) {
    KU_ASSERT(pkType.getLogicalTypeID() != LogicalTypeID::SERIAL);
    uint64_t numRows;
    if (readerSharedState != nullptr) {
        KU_ASSERT(distinctSharedState == nullptr);
        auto scanSharedState =
            readerSharedState->funcState->ptrCast<function::BaseScanSharedState>();
        numRows = scanSharedState->getNumRows();
    } else {
        KU_ASSERT(distinctSharedState);
        numRows = distinctSharedState->getFactorizedTable()->getNumTuples();
    }
    pkIndex->bulkReserve(numRows);
    globalIndexBuilder = IndexBuilder(std::make_shared<IndexBuilderSharedState>(pkIndex));
}

void NodeBatchInsert::appendIncompleteNodeGroup(std::unique_ptr<ChunkedNodeGroup> localNodeGroup,
    std::optional<IndexBuilder>& indexBuilder, ExecutionContext* context) {
    std::unique_lock xLck{sharedState->mtx};
    auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    if (!nodeSharedState->sharedNodeGroup) {
        nodeSharedState->sharedNodeGroup = std::move(localNodeGroup);
        return;
    }
    auto numNodesAppended =
        nodeSharedState->sharedNodeGroup->append(localNodeGroup.get(), 0 /* offsetInNodeGroup */);
    if (nodeSharedState->sharedNodeGroup->isFull()) {
        node_group_idx_t nodeGroupIdx = nodeSharedState->getNextNodeGroupIdxWithoutLock();
        writeAndResetNodeGroup(nodeGroupIdx, context, nodeSharedState->sharedNodeGroup,
            indexBuilder);
    }
    if (numNodesAppended < localNodeGroup->getNumRows()) {
        nodeSharedState->sharedNodeGroup->append(localNodeGroup.get(), numNodesAppended);
    }
}

void NodeBatchInsert::initGlobalStateInternal(ExecutionContext* context) {
    auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    if (nodeSharedState->pkType.getLogicalTypeID() != LogicalTypeID::SERIAL) {
        nodeSharedState->initPKIndex(context);
    }
    // Set initial node group index, which should be the last one available on disk which is not
    // full, or the next index.
    auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(nodeSharedState->table);
    auto numExistingNodeGroups = nodeTable->getNumNodeGroups(context->clientContext->getTx());
    if (numExistingNodeGroups > 0 &&
        nodeTable->getNumTuplesInNodeGroup(context->clientContext->getTx(),
            numExistingNodeGroups - 1) < StorageConstants::NODE_GROUP_SIZE) {
        nodeSharedState->currentNodeGroupIdx = numExistingNodeGroups - 1;
    } else {
        nodeSharedState->currentNodeGroupIdx = numExistingNodeGroups;
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
        auto originalSelVector = nodeLocalState->columnState->getSelVectorShared();
        copyToNodeGroup(context);
        nodeLocalState->columnState->setSelVector(originalSelVector);
    }
    if (nodeLocalState->nodeGroup->getNumRows() > 0) {
        appendIncompleteNodeGroup(std::move(nodeLocalState->nodeGroup),
            nodeLocalState->localIndexBuilder, context);
    }
    if (nodeLocalState->localIndexBuilder) {
        KU_ASSERT(token);
        token->quit();
        nodeLocalState->localIndexBuilder->finishedProducing();
    }
}

void NodeBatchInsert::writeAndResetNewNodeGroup(node_group_idx_t nodeGroupIdx,
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

common::offset_t NodeBatchInsert::writeToExistingNodeGroup(transaction::Transaction* transaction,
    node_group_idx_t nodeGroupIdx, std::optional<IndexBuilder>& indexBuilder,
    column_id_t pkColumnID, NodeTable* table, ChunkedNodeGroup* nodeGroup) {
    auto numExistingTuplesInChunk = table->getNumTuplesInNodeGroup(transaction, nodeGroupIdx);
    auto numRowsToWrite = std::min(StorageConstants::NODE_GROUP_SIZE - numExistingTuplesInChunk,
        nodeGroup->getNumRows());
    auto nodeOffset =
        StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx) + numExistingTuplesInChunk;
    if (indexBuilder) {
        indexBuilder->insert(nodeGroup->getColumnChunkUnsafe(pkColumnID), nodeOffset,
            numRowsToWrite);
    }
    auto nodeInfo = ku_dynamic_cast<BatchInsertInfo*, NodeBatchInsertInfo*>(info.get());
    LocalNodeNG localNodeGroup(StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx),
        nodeInfo->columnTypes);
    auto& insertChunks = localNodeGroup.getInsertChunks();
    insertChunks.append(numExistingTuplesInChunk, nodeGroup, numRowsToWrite);
    table->prepareCommitNodeGroup(nodeGroupIdx, transaction, &localNodeGroup);
    return numRowsToWrite;
}

void NodeBatchInsert::clearToIndex(std::unique_ptr<ChunkedNodeGroup>& nodeGroup,
    common::offset_t startIndexInGroup) {
    // Create a new chunked node group and move the unwritten values to it
    // TODO(bmwinger): Can probably re-use the chunk and shift the values
    auto oldNodeGroup = std::move(nodeGroup);
    auto nodeInfo = ku_dynamic_cast<BatchInsertInfo*, NodeBatchInsertInfo*>(info.get());
    nodeGroup = NodeGroupFactory::createNodeGroup(ColumnDataFormat::REGULAR, nodeInfo->columnTypes,
        info->compressionEnabled);
    nodeGroup->append(oldNodeGroup.get(), startIndexInGroup);
}

void NodeBatchInsert::copyToNodeGroup(ExecutionContext* context) {
    auto numAppendedTuples = 0ul;
    auto nodeLocalState =
        ku_dynamic_cast<BatchInsertLocalState*, NodeBatchInsertLocalState*>(localState.get());
    auto numTuplesToAppend = nodeLocalState->columnState->getSelVector().getSelSize();
    auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    while (numAppendedTuples < numTuplesToAppend) {
        auto numAppendedTuplesInNodeGroup = nodeLocalState->nodeGroup->append(
            nodeLocalState->columnVectors, nodeLocalState->columnState->getSelVectorUnsafe(),
            numTuplesToAppend - numAppendedTuples);
        numAppendedTuples += numAppendedTuplesInNodeGroup;
        if (nodeLocalState->nodeGroup->isFull()) {
            writeAndResetNodeGroup(nodeSharedState->getNextNodeGroupIdx(), context,
                nodeLocalState->nodeGroup, nodeLocalState->localIndexBuilder);
        }
        if (numAppendedTuples < numTuplesToAppend) {
            nodeLocalState->columnState->slice((offset_t)numAppendedTuplesInNodeGroup);
        }
    }
    sharedState->incrementNumRows(numAppendedTuples);
}

void NodeBatchInsert::writeAndResetNodeGroup(node_group_idx_t nodeGroupIdx,
    ExecutionContext* context, std::unique_ptr<storage::ChunkedNodeGroup>& nodeGroup,
    std::optional<IndexBuilder>& indexBuilder) {
    auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(sharedState->table);

    if (nodeGroupIdx >= nodeTable->getNumNodeGroups(context->clientContext->getTx())) {
        writeAndResetNewNodeGroup(nodeGroupIdx, indexBuilder, nodeSharedState->pkColumnIdx,
            nodeTable, nodeGroup.get());
    } else {
        KU_ASSERT(nodeGroupIdx == nodeTable->getNumNodeGroups(context->clientContext->getTx()) - 1);
        auto valuesWritten = writeToExistingNodeGroup(context->clientContext->getTx(), nodeGroupIdx,
            indexBuilder, nodeSharedState->pkColumnIdx, nodeTable, nodeGroup.get());
        clearToIndex(nodeGroup, valuesWritten);
    }
}

void NodeBatchInsert::finalize(ExecutionContext* context) {
    auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    nodeSharedState->updateNumTuplesForTable();
    if (nodeSharedState->sharedNodeGroup) {
        while (nodeSharedState->sharedNodeGroup->getNumRows() > 0) {
            writeAndResetNodeGroup(nodeSharedState->getNextNodeGroupIdx(), context,
                nodeSharedState->sharedNodeGroup, nodeSharedState->globalIndexBuilder);
        }
    }
    if (nodeSharedState->globalIndexBuilder) {
        nodeSharedState->globalIndexBuilder->finalize(context);
    }
    sharedState->logBatchInsertWALRecord();
    auto outputMsg = stringFormat("{} tuples have been copied to the {} table.",
        sharedState->getNumRows(), info->tableEntry->getName());
    FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), outputMsg,
        context->clientContext->getMemoryManager());
}
} // namespace processor
} // namespace kuzu
