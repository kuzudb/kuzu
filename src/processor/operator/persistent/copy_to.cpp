#include "processor/operator/persistent/copy_to.h"

namespace kuzu {
namespace processor {

void CopyTo::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState = copyFunc.copyToInitLocal(*context->clientContext, *bindData, *resultSet);
}

void CopyTo::finalize(ExecutionContext* /*context*/) {
    copyFunc.copyToFinalize(*sharedState);
}

void CopyTo::executeInternal(processor::ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        copyFunc.copyToSink(*sharedState, *localState, *bindData);
    }
    copyFunc.copyToCombine(*sharedState, *localState);
}

bool CopyTo::isParallel() const {
    return bindData->canParallel;
}

} // namespace processor
} // namespace kuzu
