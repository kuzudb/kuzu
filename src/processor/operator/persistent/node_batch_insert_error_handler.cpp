#include "processor/operator/persistent/node_batch_insert_error_handler.h"

#include "processor/execution_context.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

NodeBatchInsertErrorHandler::NodeBatchInsertErrorHandler(ExecutionContext* context,
    common::LogicalTypeID pkType, storage::NodeTable* nodeTable, bool ignoreErrors,
    std::shared_ptr<common::row_idx_t> sharedErrorCounter, std::mutex* sharedErrorCounterMtx)
    : nodeTable(nodeTable), context(context),
      keyVector(std::make_shared<ValueVector>(pkType, context->clientContext->getMemoryManager())),
      offsetVector(std::make_shared<ValueVector>(LogicalTypeID::INTERNAL_ID,
          context->clientContext->getMemoryManager())),
      baseErrorHandler(context, ignoreErrors, sharedErrorCounter, sharedErrorCounterMtx) {
    keyVector->state = DataChunkState::getSingleValueDataChunkState();
    offsetVector->state = DataChunkState::getSingleValueDataChunkState();
}

void NodeBatchInsertErrorHandler::deleteCurrentErroneousRow() {
    storage::NodeTableDeleteState deleteState{
        *offsetVector,
        *keyVector,
    };
    nodeTable->delete_(context->clientContext->getTransaction(), deleteState);
}

void NodeBatchInsertErrorHandler::flushStoredErrors() {
    baseErrorHandler.flushStoredErrors();
}

} // namespace processor
} // namespace kuzu
