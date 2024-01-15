#include "processor/operator/persistent/copy_node.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "common/types/types.h"
#include "function/table_functions/scan_functions.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void CopyNodeSharedState::init(ExecutionContext* context) {
    wal->logCopyTableRecord(table->getTableID(), TableType::NODE);
    wal->flushAllPages();
    if (pkType.getLogicalTypeID() != LogicalTypeID::SERIAL) {
        auto indexFName = StorageUtils::getNodeIndexFName(context->clientContext->getVFSUnsafe(),
            wal->getDirectory(), table->getTableID(), FileVersionType::ORIGINAL);
        pkIndex = std::make_unique<PrimaryKeyIndexBuilder>(
            indexFName, pkType.getPhysicalType(), context->clientContext->getVFSUnsafe());
        uint64_t numRows;
        if (readerSharedState != nullptr) {
            KU_ASSERT(distinctSharedState == nullptr);
            auto scanSharedState =
                reinterpret_cast<function::ScanSharedState*>(readerSharedState->sharedState.get());
            numRows = scanSharedState->numRows;
        } else {
            numRows = distinctSharedState->getFactorizedTable()->getNumTuples();
        }
        pkIndex->bulkReserve(numRows);
        globalIndexBuilder = IndexBuilder(std::make_shared<IndexBuilderSharedState>(pkIndex.get()));
    }
}

void CopyNodeSharedState::appendIncompleteNodeGroup(
    std::unique_ptr<NodeGroup> localNodeGroup, std::optional<IndexBuilder>& indexBuilder) {
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
            nodeGroupIdx, indexBuilder, pkColumnIdx, table, sharedNodeGroup.get());
    }
    if (numNodesAppended < localNodeGroup->getNumRows()) {
        sharedNodeGroup->append(localNodeGroup.get(), numNodesAppended);
    }
}

void CopyNodeSharedState::finalize(ExecutionContext* context) {
    calculateNumTuples();
    if (sharedNodeGroup) {
        auto nodeGroupIdx = getNextNodeGroupIdx();
        CopyNode::writeAndResetNodeGroup(
            nodeGroupIdx, globalIndexBuilder, pkColumnIdx, table, sharedNodeGroup.get());
    }
    table->getNodeStatisticsAndDeletedIDs()->setNumTuplesForTable(table->getTableID(), numTuples);
    if (globalIndexBuilder) {
        globalIndexBuilder->finalize(context);
    }
}

static bool isEmptyTable(NodeTable* nodeTable) {
    auto nodesStatistics = nodeTable->getNodeStatisticsAndDeletedIDs();
    return nodesStatistics->getNodeStatisticsAndDeletedIDs(nodeTable->getTableID())
               ->getNumTuples() == 0;
}

void CopyNode::initGlobalStateInternal(ExecutionContext* context) {
    if (!isEmptyTable(info->table)) {
        throw CopyException(ExceptionMessage::notAllowCopyOnNonEmptyTableException());
    }
    sharedState->init(context);
}

void CopyNode::initLocalStateInternal(ResultSet* resultSet, ExecutionContext*) {
    std::shared_ptr<DataChunkState> state;
    for (auto& pos : info->columnPositions) {
        if (pos.isValid()) {
            state = resultSet->getValueVector(pos)->state;
        }
    }

    // NOLINTBEGIN(bugprone-unchecked-optional-access)
    if (sharedState->globalIndexBuilder) {
        localIndexBuilder = sharedState->globalIndexBuilder.value().clone();
    }
    // NOLINTEND(bugprone-unchecked-optional-access)

    KU_ASSERT(state != nullptr);
    for (auto i = 0u; i < info->columnPositions.size(); ++i) {
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
    std::optional<ProducerToken> token;
    if (localIndexBuilder) {
        token = localIndexBuilder->getProducerToken();
    }

    while (children[0]->getNextTuple(context)) {
        auto originalSelVector = columnState->selVector;
        copyToNodeGroup();
        columnState->selVector = std::move(originalSelVector);
    }
    if (localNodeGroup->getNumRows() > 0) {
        sharedState->appendIncompleteNodeGroup(std::move(localNodeGroup), localIndexBuilder);
    }
    if (localIndexBuilder) {
        KU_ASSERT(token);
        token->quit();
        localIndexBuilder->finishedProducing();
    }
}

void CopyNode::writeAndResetNodeGroup(node_group_idx_t nodeGroupIdx,
    std::optional<IndexBuilder>& indexBuilder, column_id_t pkColumnID, NodeTable* table,
    NodeGroup* nodeGroup) {
    nodeGroup->finalize(nodeGroupIdx);
    if (indexBuilder) {
        auto nodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
        indexBuilder->insert(
            nodeGroup->getColumnChunk(pkColumnID), nodeOffset, nodeGroup->getNumRows());
    }
    table->append(nodeGroup);
    nodeGroup->resetToEmpty();
}

void CopyNodeSharedState::calculateNumTuples() {
    numTuples = StorageUtils::getStartOffsetOfNodeGroup(getCurNodeGroupIdx());
    if (sharedNodeGroup) {
        numTuples += sharedNodeGroup->getNumRows();
    }
}

void CopyNode::finalize(ExecutionContext* context) {
    sharedState->finalize(context);
    for (auto relTable : info->fwdRelTables) {
        relTable->resizeColumns(context->clientContext->getTx(), RelDataDirection::FWD,
            sharedState->getCurNodeGroupIdx());
    }
    for (auto relTable : info->bwdRelTables) {
        relTable->resizeColumns(context->clientContext->getTx(), RelDataDirection::BWD,
            sharedState->getCurNodeGroupIdx());
    }
    auto outputMsg = stringFormat("{} number of tuples has been copied to table: {}.",
        sharedState->numTuples, info->tableName.c_str());
    FactorizedTableUtils::appendStringToTable(
        sharedState->fTable.get(), outputMsg, context->clientContext->getMemoryManager());
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
            writeAndResetNodeGroup(nodeGroupIdx, localIndexBuilder, sharedState->pkColumnIdx,
                sharedState->table, localNodeGroup.get());
        }
        if (numAppendedTuples < numTuplesToAppend) {
            columnState->slice((offset_t)numAppendedTuplesInNodeGroup);
        }
    }
}

} // namespace processor
} // namespace kuzu
