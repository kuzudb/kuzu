#include "processor/operator/persistent/node_batch_insert.h"

#include "common/cast.h"
#include "common/constants.h"
#include "common/string_format.h"
#include "function/table/scan_functions.h"
#include "processor/execution_context.h"
#include "processor/operator/persistent/index_builder.h"
#include "processor/result/factorized_table.h"
#include "processor/result/factorized_table_util.h"
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
    uint64_t numRows = 0;
    if (readerSharedState != nullptr) {
        KU_ASSERT(distinctSharedState == nullptr);
        const auto scanSharedState =
            readerSharedState->funcState->ptrCast<function::BaseScanSharedState>();
        numRows = scanSharedState->getNumRows();
    } else {
        KU_ASSERT(distinctSharedState);
        numRows = distinctSharedState->getFactorizedTable()->getNumTuples();
    }
    auto* nodeTable = ku_dynamic_cast<NodeTable*>(table);
    nodeTable->getPKIndex()->bulkReserve(numRows);
    globalIndexBuilder = IndexBuilder(
        std::make_shared<IndexBuilderSharedState>(context->clientContext->getTx(), nodeTable));
}

void NodeBatchInsert::initGlobalStateInternal(ExecutionContext* context) {
    const auto nodeSharedState = ku_dynamic_cast<NodeBatchInsertSharedState*>(sharedState.get());
    nodeSharedState->initPKIndex(context);
}

void NodeBatchInsert::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    auto nodeInfo = info->ptrCast<NodeBatchInsertInfo>();

    const auto nodeSharedState = ku_dynamic_cast<NodeBatchInsertSharedState*>(sharedState.get());
    localState = std::make_unique<NodeBatchInsertLocalState>();
    const auto nodeLocalState = localState->ptrCast<NodeBatchInsertLocalState>();
    KU_ASSERT(nodeSharedState->globalIndexBuilder);
    nodeLocalState->localIndexBuilder = nodeSharedState->globalIndexBuilder->clone();

    auto* nodeTable = ku_dynamic_cast<NodeTable*>(sharedState->table);
    nodeLocalState->errorHandler =
        NodeBatchInsertErrorHandler{context, nodeSharedState->pkType.getLogicalTypeID(), nodeTable,
            context->clientContext->getWarningContext().getIgnoreErrorsOption(),
            sharedState->numErroredRows, &sharedState->erroredRowMutex};

    const auto numColumns = nodeInfo->columnEvaluators.size();
    nodeLocalState->columnVectors.resize(numColumns);

    for (auto i = 0u; i < numColumns; ++i) {
        auto& evaluator = nodeInfo->columnEvaluators[i];
        evaluator->init(*resultSet, context->clientContext);
        nodeLocalState->columnVectors[i] = evaluator->resultVector.get();
    }
    nodeLocalState->chunkedGroup = std::make_unique<ChunkedNodeGroup>(
        *context->clientContext->getMemoryManager(), nodeInfo->columnTypes,
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
        copyToNodeGroup(context->clientContext->getTx(),
            context->clientContext->getMemoryManager());
        nodeLocalState->columnState->setSelVector(originalSelVector);
    }
    if (nodeLocalState->chunkedGroup->getNumRows() > 0) {
        appendIncompleteNodeGroup(context->clientContext->getTx(),
            std::move(nodeLocalState->chunkedGroup), nodeLocalState->localIndexBuilder,
            context->clientContext->getMemoryManager());
    }
    if (nodeLocalState->localIndexBuilder) {
        KU_ASSERT(token);
        token->quit();

        KU_ASSERT(nodeLocalState->errorHandler.has_value());
        nodeLocalState->localIndexBuilder->finishedProducing(nodeLocalState->errorHandler.value());
        nodeLocalState->errorHandler->flushStoredErrors();
    }
}

void NodeBatchInsert::copyToNodeGroup(transaction::Transaction* transaction,
    MemoryManager* mm) const {
    auto numAppendedTuples = 0ul;
    const auto nodeLocalState = ku_dynamic_cast<NodeBatchInsertLocalState*>(localState.get());
    const auto numTuplesToAppend = nodeLocalState->columnState->getSelVector().getSelSize();
    while (numAppendedTuples < numTuplesToAppend) {
        const auto numAppendedTuplesInNodeGroup = nodeLocalState->chunkedGroup->append(
            &transaction::DUMMY_TRANSACTION, nodeLocalState->columnVectors, numAppendedTuples,
            numTuplesToAppend - numAppendedTuples);
        numAppendedTuples += numAppendedTuplesInNodeGroup;
        if (nodeLocalState->chunkedGroup->isFullOrOnDisk()) {
            writeAndResetNodeGroup(transaction, nodeLocalState->chunkedGroup,
                nodeLocalState->localIndexBuilder, mm);
        }
    }
    sharedState->incrementNumRows(numAppendedTuples);
}

void NodeBatchInsert::clearToIndex(MemoryManager* mm, std::unique_ptr<ChunkedNodeGroup>& nodeGroup,
    offset_t startIndexInGroup) const {
    // Create a new chunked node group and move the unwritten values to it
    // TODO(bmwinger): Can probably re-use the chunk and shift the values
    const auto oldNodeGroup = std::move(nodeGroup);
    const auto nodeInfo = info->ptrCast<NodeBatchInsertInfo>();
    nodeGroup = std::make_unique<ChunkedNodeGroup>(*mm, nodeInfo->columnTypes,
        info->compressionEnabled, StorageConstants::NODE_GROUP_SIZE, 0, ResidencyState::IN_MEMORY);
    nodeGroup->append(&transaction::DUMMY_TRANSACTION, *oldNodeGroup, startIndexInGroup,
        oldNodeGroup->getNumRows() - startIndexInGroup);
}

void NodeBatchInsert::writeAndResetNodeGroup(transaction::Transaction* transaction,
    std::unique_ptr<ChunkedNodeGroup>& nodeGroup, std::optional<IndexBuilder>& indexBuilder,
    MemoryManager* mm) const {
    const auto nodeSharedState = ku_dynamic_cast<NodeBatchInsertSharedState*>(sharedState.get());
    const auto nodeLocalState = localState->ptrCast<NodeBatchInsertLocalState>();
    const auto nodeTable = ku_dynamic_cast<NodeTable*>(sharedState->table);
    auto nodeInfo = info->ptrCast<NodeBatchInsertInfo>();

    // we only need to write the main data in the chunked node group, the extra data is only used
    // during the lifetime of this operator to populate error messages
    ChunkedNodeGroup sliceToWriteToDisk(*nodeGroup, nodeInfo->outputDataColumns);
    auto [nodeOffset, numRowsWritten] =
        nodeTable->appendToLastNodeGroup(transaction, sliceToWriteToDisk);
    nodeGroup->merge(sliceToWriteToDisk, nodeInfo->outputDataColumns);

    if (indexBuilder) {
        KU_ASSERT(nodeLocalState->errorHandler.has_value());
        std::vector<ColumnChunkData*> warningChunkData;
        for (const auto warningDataColumn : nodeInfo->warningDataColumns) {
            warningChunkData.push_back(&nodeGroup->getColumnChunk(warningDataColumn).getData());
        }
        indexBuilder->insert(nodeGroup->getColumnChunk(nodeSharedState->pkColumnID).getData(),
            warningChunkData, nodeOffset, numRowsWritten, nodeLocalState->errorHandler.value());
    }
    if (numRowsWritten == nodeGroup->getNumRows()) {
        nodeGroup->resetToEmpty();
    } else {
        clearToIndex(mm, nodeGroup, numRowsWritten);
    }
}

void NodeBatchInsert::appendIncompleteNodeGroup(transaction::Transaction* transaction,
    std::unique_ptr<ChunkedNodeGroup> localNodeGroup, std::optional<IndexBuilder>& indexBuilder,
    MemoryManager* mm) const {
    std::unique_lock xLck{sharedState->mtx};
    const auto nodeSharedState = ku_dynamic_cast<NodeBatchInsertSharedState*>(sharedState.get());
    if (!nodeSharedState->sharedNodeGroup) {
        nodeSharedState->sharedNodeGroup = std::move(localNodeGroup);
        return;
    }
    auto numNodesAppended =
        nodeSharedState->sharedNodeGroup->append(&transaction::DUMMY_TRANSACTION, *localNodeGroup,
            0 /* offsetInNodeGroup */, localNodeGroup->getNumRows());
    while (nodeSharedState->sharedNodeGroup->isFullOrOnDisk()) {
        writeAndResetNodeGroup(transaction, nodeSharedState->sharedNodeGroup, indexBuilder, mm);
        if (numNodesAppended < localNodeGroup->getNumRows()) {
            numNodesAppended += nodeSharedState->sharedNodeGroup->append(
                &transaction::DUMMY_TRANSACTION, *localNodeGroup, numNodesAppended,
                localNodeGroup->getNumRows() - numNodesAppended);
        }
    }
    KU_ASSERT(numNodesAppended == localNodeGroup->getNumRows());
}

void NodeBatchInsert::finalize(ExecutionContext* context) {
    const auto nodeSharedState = ku_dynamic_cast<NodeBatchInsertSharedState*>(sharedState.get());
    if (nodeSharedState->sharedNodeGroup) {
        while (nodeSharedState->sharedNodeGroup->getNumRows() > 0) {
            writeAndResetNodeGroup(context->clientContext->getTx(),
                nodeSharedState->sharedNodeGroup, nodeSharedState->globalIndexBuilder,
                context->clientContext->getMemoryManager());
        }
    }
    if (nodeSharedState->globalIndexBuilder) {
        auto* localNodeState = ku_dynamic_cast<NodeBatchInsertLocalState*>(localState.get());
        KU_ASSERT(localNodeState->errorHandler.has_value());
        nodeSharedState->globalIndexBuilder->finalize(context,
            localNodeState->errorHandler.value());
        localNodeState->errorHandler->flushStoredErrors();
    }

    // we want to flush all index errors before children call finalize
    // as the children are resposible for populating the errors and sending it to the warning
    // context
    PhysicalOperator::finalize(context);
}

void NodeBatchInsert::finalizeInternal(ExecutionContext* context) {
    auto outputMsg = stringFormat("{} tuples have been copied to the {} table.",
        sharedState->getNumRows() - sharedState->getNumErroredRows(), info->tableEntry->getName());
    FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), outputMsg,
        context->clientContext->getMemoryManager());

    const auto warningCount =
        context->clientContext->getWarningContextUnsafe().getWarningCount(context->queryID);
    if (warningCount > 0) {
        auto warningMsg =
            stringFormat("{} warnings encountered during copy. Use 'CALL "
                         "show_warnings() RETURN *' to view the actual warnings. Query ID: {}",
                warningCount, context->queryID);
        FactorizedTableUtils::appendStringToTable(sharedState->fTable.get(), warningMsg,
            context->clientContext->getMemoryManager());
    }
}

} // namespace processor
} // namespace kuzu
