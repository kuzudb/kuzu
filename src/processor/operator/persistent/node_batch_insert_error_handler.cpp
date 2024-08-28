#include "processor/operator/persistent/node_batch_insert_error_handler.h"

#include "processor/execution_context.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {
NodeBatchInsertErrorHandler::NodeBatchInsertErrorHandler(ExecutionContext* context,
    common::LogicalTypeID pkType, storage::NodeTable* nodeTable,
    std::shared_ptr<common::row_idx_t> sharedErrorCounter, std::mutex* sharedErrorCounterMtx)
    : ignoreErrors(context->clientContext->getClientConfig()->ignoreCopyErrors),
      warningLimit(
          std::min(context->clientContext->getClientConfig()->warningLimit, LOCAL_WARNING_LIMIT)),
      context(context), pkType(pkType), nodeTable(nodeTable), queryID(context->queryID),
      sharedErrorCounterMtx(sharedErrorCounterMtx),
      sharedErrorCounter(std::move(sharedErrorCounter)) {}

void NodeBatchInsertErrorHandler::addNewVectors() {
    offsetVector.push_back(std::make_shared<ValueVector>(LogicalTypeID::INTERNAL_ID,
        context->clientContext->getMemoryManager()));
    offsetVector.back()->state = DataChunkState::getSingleValueDataChunkState();
    keyVector.push_back(
        std::make_shared<ValueVector>(pkType, context->clientContext->getMemoryManager()));
    keyVector.back()->state = DataChunkState::getSingleValueDataChunkState();
}

void NodeBatchInsertErrorHandler::flushStoredErrors() {
    std::vector<PopulatedCSVError> populatedErrors;

    for (row_idx_t i = 0; i < getNumErrors(); ++i) {
        storage::NodeTableDeleteState deleteState{
            *offsetVector[i],
            *keyVector[i],
        };
        nodeTable->delete_(context->clientContext->getTx(), deleteState);

        populatedErrors.push_back({
            .message = errorMessages[i],
            .filePath = "",
            .skippedLine = "",
            .lineNumber = 0,
        });
    }
    context->appendWarningMessages(populatedErrors, queryID);

    {
        common::UniqLock lockGuard{*sharedErrorCounterMtx};
        *sharedErrorCounter += getNumErrors();
    }

    clearErrors();
}

void NodeBatchInsertErrorHandler::clearErrors() {
    offsetVector.clear();
    keyVector.clear();
    errorMessages.clear();
}

row_idx_t NodeBatchInsertErrorHandler::getNumErrors() const {
    KU_ASSERT(offsetVector.size() == keyVector.size());
    KU_ASSERT(offsetVector.size() == errorMessages.size());
    return offsetVector.size();
}

} // namespace processor
} // namespace kuzu
