#include "processor/operator/persistent/copy_to.h"

namespace kuzu {
namespace processor {

void CopyTo::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState->init(info.get(), context->memoryManager, resultSet);
}

void CopyTo::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->init(info.get(), context->memoryManager);
}

void CopyTo::finalize(ExecutionContext* /*context*/) {
    sharedState->finalize();
}

void CopyTo::executeInternal(processor::ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        localState->sink(sharedState.get(), info.get());
    }
    localState->finalize(sharedState.get());
}

} // namespace processor
} // namespace kuzu
