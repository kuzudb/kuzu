#include "processor/operator/persistent/node_batch_insert.h"

#include "common/cast.h"
#include "common/constants.h"
#include "common/string_format.h"
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

std::string NodeBatchInsertPrintInfo::toString() const {
    std::string result = "Table Name: ";
    result += tableName;
    return result;
}

void NodeBatchInsertSharedState::initPKIndex(const ExecutionContext* context) {
    uint64_t numRows;
    if (readerSharedState != nullptr) {
        KU_ASSERT(distinctSharedState == nullptr);
        const auto scanSharedState =
            readerSharedState->funcState->ptrCast<function::BaseScanSharedState>();
        numRows = scanSharedState->getNumRows();
    } else {
        KU_ASSERT(distinctSharedState);
        numRows = distinctSharedState->getFactorizedTable()->getNumTuples();
    }
    auto* nodeTable = ku_dynamic_cast<Table*, NodeTable*>(table);
    nodeTable->getPKIndex()->bulkReserve(numRows);
    globalIndexBuilder = IndexBuilder(
        std::make_shared<IndexBuilderSharedState>(context->clientContext->getTx(), nodeTable));
}

void NodeBatchInsert::initGlobalStateInternal(ExecutionContext* context) {
    const auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    nodeSharedState->initPKIndex(context);
}

void NodeBatchInsert::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    auto nodeInfo = info->ptrCast<NodeBatchInsertInfo>();

    const auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    localState = std::make_unique<NodeBatchInsertLocalState>();
    const auto nodeLocalState = localState->ptrCast<NodeBatchInsertLocalState>();
    // NOLINTBEGIN(bugprone-unchecked-optional-access)
    if (nodeSharedState->globalIndexBuilder) {
        nodeLocalState->localIndexBuilder = nodeSharedState->globalIndexBuilder.value().clone();

        auto* nodeTable = ku_dynamic_cast<Table*, NodeTable*>(sharedState->table);
        nodeLocalState->errorHandler = NodeBatchInsertErrorHandler{context,
            nodeSharedState->pkType.getLogicalTypeID(), nodeTable, nodeInfo->ignoreErrors,
            sharedState->numErroredRows, &sharedState->erroredRowMutex};
    }
    // NOLINTEND(bugprone-unchecked-optional-access)

    const auto numColumns = nodeInfo->columnEvaluators.size();
    nodeLocalState->columnVectors.resize(numColumns);

    for (auto i = 0u; i < numColumns; ++i) {
        auto& evaluator = nodeInfo->columnEvaluators[i];
        evaluator->init(*resultSet, context->clientContext);
        nodeLocalState->columnVectors[i] = evaluator->resultVector.get();
    }
    nodeLocalState->chunkedGroup = std::make_unique<ChunkedNodeGroup>(nodeInfo->columnTypes,
        info->compressionEnabled, StorageConstants::NODE_GROUP_SIZE, 0, ResidencyState::IN_MEMORY);
    KU_ASSERT(resultSet->dataChunks[0]);
    nodeLocalState->columnState = resultSet->dataChunks[0]->state;
}

void NodeBatchInsert::executeInternal(ExecutionContext* context) {
    std::optional<ProducerToken> token;
    auto nodeLocalState = localState->ptrCast<NodeBatchInsertLocalState>();
    auto nodeInfo = info->ptrCast<NodeBatchInsertInfo>();
    if (nodeLocalState->localIndexBuilder) {
        token = nodeLocalState->localIndexBuilder->getProducerToken();
    }

    while (children[0]->getNextTuple(context)) {
        auto originalSelVector = nodeLocalState->columnState->getSelVectorShared();
        // Evaluate expressions if needed.
        auto numTuples = nodeLocalState->columnState->getSelVector().getSelSize();
        for (auto i = 0u; i < nodeInfo->evaluateTypes.size(); ++i) {
            switch (nodeInfo->evaluateTypes[i]) {
            case ColumnEvaluateType::DEFAULT: {
                auto& defaultEvaluator = nodeInfo->columnEvaluators[i];
                defaultEvaluator->getLocalStateUnsafe().count = numTuples;
                defaultEvaluator->evaluate();
            } break;
            case ColumnEvaluateType::CAST: {
                nodeInfo->columnEvaluators[i]->evaluate();
            } break;
            default:
                break;
            }
        }
        copyToNodeGroup(context->clientContext->getTx());
        nodeLocalState->columnState->setSelVector(originalSelVector);
    }
    if (nodeLocalState->chunkedGroup->getNumRows() > 0) {
        appendIncompleteNodeGroup(context->clientContext->getTx(),
            std::move(nodeLocalState->chunkedGroup), nodeLocalState->localIndexBuilder);
    }
    if (nodeLocalState->localIndexBuilder) {
        KU_ASSERT(token);
        token->quit();

        KU_ASSERT(nodeLocalState->errorHandler.has_value());
        nodeLocalState->localIndexBuilder->finishedProducing(nodeLocalState->errorHandler.value());
        nodeLocalState->errorHandler->flushStoredErrors();
    }
}

void NodeBatchInsert::copyToNodeGroup(transaction::Transaction* transaction) const {
    auto numAppendedTuples = 0ul;
    const auto nodeLocalState =
        ku_dynamic_cast<BatchInsertLocalState*, NodeBatchInsertLocalState*>(localState.get());
    const auto numTuplesToAppend = nodeLocalState->columnState->getSelVector().getSelSize();
    while (numAppendedTuples < numTuplesToAppend) {
        const auto numAppendedTuplesInNodeGroup = nodeLocalState->chunkedGroup->append(
            &transaction::DUMMY_TRANSACTION, nodeLocalState->columnVectors, numAppendedTuples,
            numTuplesToAppend - numAppendedTuples);
        numAppendedTuples += numAppendedTuplesInNodeGroup;
        if (nodeLocalState->chunkedGroup->isFullOrOnDisk()) {
            writeAndResetNodeGroup(transaction, nodeLocalState->chunkedGroup,
                nodeLocalState->localIndexBuilder);
        }
    }
    sharedState->incrementNumRows(numAppendedTuples);
}

void NodeBatchInsert::clearToIndex(std::unique_ptr<ChunkedNodeGroup>& nodeGroup,
    offset_t startIndexInGroup) const {
    // Create a new chunked node group and move the unwritten values to it
    // TODO(bmwinger): Can probably re-use the chunk and shift the values
    const auto oldNodeGroup = std::move(nodeGroup);
    const auto nodeInfo = info->ptrCast<NodeBatchInsertInfo>();
    nodeGroup = std::make_unique<ChunkedNodeGroup>(nodeInfo->columnTypes, info->compressionEnabled,
        StorageConstants::NODE_GROUP_SIZE, 0, ResidencyState::IN_MEMORY);
    nodeGroup->append(&transaction::DUMMY_TRANSACTION, *oldNodeGroup, startIndexInGroup,
        oldNodeGroup->getNumRows() - startIndexInGroup);
}

void NodeBatchInsert::writeAndResetNodeGroup(transaction::Transaction* transaction,
    std::unique_ptr<ChunkedNodeGroup>& nodeGroup, std::optional<IndexBuilder>& indexBuilder) const {
    const auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    const auto nodeLocalState = localState->ptrCast<NodeBatchInsertLocalState>();
    const auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(sharedState->table);
    auto [nodeOffset, numRowsWritten] = nodeTable->appendToLastNodeGroup(transaction, *nodeGroup);
    if (indexBuilder) {
        KU_ASSERT(nodeLocalState->errorHandler.has_value());
        indexBuilder->insert(nodeGroup->getColumnChunk(nodeSharedState->pkColumnID).getData(),
            nodeOffset, numRowsWritten, nodeLocalState->errorHandler.value());
    }
    if (numRowsWritten == nodeGroup->getNumRows()) {
        nodeGroup->resetToEmpty();
    } else {
        clearToIndex(nodeGroup, numRowsWritten);
    }
}

void NodeBatchInsert::appendIncompleteNodeGroup(transaction::Transaction* transaction,
    std::unique_ptr<ChunkedNodeGroup> localNodeGroup,
    std::optional<IndexBuilder>& indexBuilder) const {
    std::unique_lock xLck{sharedState->mtx};
    const auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    if (!nodeSharedState->sharedNodeGroup) {
        nodeSharedState->sharedNodeGroup = std::move(localNodeGroup);
        return;
    }
    auto numNodesAppended =
        nodeSharedState->sharedNodeGroup->append(&transaction::DUMMY_TRANSACTION, *localNodeGroup,
            0 /* offsetInNodeGroup */, localNodeGroup->getNumRows());
    while (nodeSharedState->sharedNodeGroup->isFullOrOnDisk()) {
        writeAndResetNodeGroup(transaction, nodeSharedState->sharedNodeGroup, indexBuilder);
        if (numNodesAppended < localNodeGroup->getNumRows()) {
            numNodesAppended += nodeSharedState->sharedNodeGroup->append(
                &transaction::DUMMY_TRANSACTION, *localNodeGroup, numNodesAppended,
                localNodeGroup->getNumRows() - numNodesAppended);
        }
    }
    KU_ASSERT(numNodesAppended == localNodeGroup->getNumRows());
}

void NodeBatchInsert::finalizeInternal(ExecutionContext* context) {
    const auto nodeSharedState =
        ku_dynamic_cast<BatchInsertSharedState*, NodeBatchInsertSharedState*>(sharedState.get());
    if (nodeSharedState->sharedNodeGroup) {
        while (nodeSharedState->sharedNodeGroup->getNumRows() > 0) {
            writeAndResetNodeGroup(context->clientContext->getTx(),
                nodeSharedState->sharedNodeGroup, nodeSharedState->globalIndexBuilder);
        }
    }
    if (nodeSharedState->globalIndexBuilder) {
        auto* localNodeState =
            ku_dynamic_cast<BatchInsertLocalState*, NodeBatchInsertLocalState*>(localState.get());
        KU_ASSERT(localNodeState->errorHandler.has_value());
        nodeSharedState->globalIndexBuilder->finalize(context,
            localNodeState->errorHandler.value());
        localNodeState->errorHandler->flushStoredErrors();
    }

    auto outputMsg = stringFormat("{} tuples have been copied to the {} table.",
        sharedState->getNumRows() - sharedState->getNumErroredRows(), info->tableEntry->getName());
    FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), outputMsg,
        context->clientContext->getMemoryManager());

    const auto warningCount = context->warningCount;
    bool atWarningLimit = (warningCount == context->clientContext->getClientConfig()->warningLimit);
    if (warningCount > 0) {
        const auto warningLimitSuffix = atWarningLimit ? "+" : "";
        auto warningMsg =
            stringFormat("{}{} warnings encountered during copy. Use 'CALL "
                         "show_warnings() RETURN *' to view the actual warnings. Query ID: {}",
                warningCount, warningLimitSuffix, context->queryID);
        FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), warningMsg,
            context->clientContext->getMemoryManager());
    }
}

} // namespace processor
} // namespace kuzu
