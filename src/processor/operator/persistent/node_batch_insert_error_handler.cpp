#include "processor/operator/persistent/node_batch_insert_error_handler.h"

#include "processor/execution_context.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {
IndexBuilderCachedError::IndexBuilderCachedError(std::string message,
    std::optional<WarningSourceData> warningData)
    : message(message), warningData(warningData) {}

NodeBatchInsertErrorHandler::NodeBatchInsertErrorHandler(ExecutionContext* context,
    common::LogicalTypeID pkType, storage::NodeTable* nodeTable, bool ignoreErrors,
    std::shared_ptr<common::row_idx_t> sharedErrorCounter, std::mutex* sharedErrorCounterMtx)
    : ignoreErrors(ignoreErrors),
      warningLimit(
          std::min(context->clientContext->getClientConfig()->warningLimit, LOCAL_WARNING_LIMIT)),
      context(context), nodeTable(nodeTable), queryID(context->queryID), currentInsertIdx(0),
      sharedErrorCounterMtx(sharedErrorCounterMtx),
      sharedErrorCounter(std::move(sharedErrorCounter)),
      keyVector(std::make_shared<ValueVector>(pkType, context->clientContext->getMemoryManager())),
      offsetVector(std::make_shared<ValueVector>(LogicalTypeID::INTERNAL_ID,
          context->clientContext->getMemoryManager())) {
    keyVector->state = DataChunkState::getSingleValueDataChunkState();
    offsetVector->state = DataChunkState::getSingleValueDataChunkState();
}

void NodeBatchInsertErrorHandler::addNewVectorsIfNeeded() {
    KU_ASSERT(currentInsertIdx <= cachedErrors.size());
    if (currentInsertIdx == cachedErrors.size()) {
        cachedErrors.emplace_back();
    }
}

void NodeBatchInsertErrorHandler::deleteCurrentErroneousRow() {
    storage::NodeTableDeleteState deleteState{
        *offsetVector,
        *keyVector,
    };
    nodeTable->delete_(context->clientContext->getTx(), deleteState);
}

void NodeBatchInsertErrorHandler::flushStoredErrors() {
    std::map<uint64_t, std::vector<CSVError>> unpopulatedErrors;

    for (row_idx_t i = 0; i < getNumErrors(); ++i) {
        auto& error = cachedErrors[i];
        CSVError warningToAdd{std::move(error.message), {}, false};
        if (error.warningData.has_value()) {
            warningToAdd.completedLine = true;
            warningToAdd.warningData = error.warningData.value();
        }
        unpopulatedErrors[warningToAdd.warningData.fileIdx].push_back(warningToAdd);
    }

    uint64_t numErrorsFlushed = 0;
    for (const auto& [fileIdx, unpopulatedErrorsByFile] : unpopulatedErrors) {
        KU_ASSERT(ignoreErrors);
        if (!unpopulatedErrorsByFile.empty()) {
            context->clientContext->getWarningContextUnsafe().appendWarningMessages(
                unpopulatedErrorsByFile);
            numErrorsFlushed += unpopulatedErrorsByFile.size();
        }
    }

    if (numErrorsFlushed > 0) {
        common::UniqLock lockGuard{*sharedErrorCounterMtx};
        *sharedErrorCounter += numErrorsFlushed;
    }

    clearErrors();
}

void NodeBatchInsertErrorHandler::clearErrors() {
    currentInsertIdx = 0;
}

row_idx_t NodeBatchInsertErrorHandler::getNumErrors() const {
    return currentInsertIdx;
}

} // namespace processor
} // namespace kuzu
