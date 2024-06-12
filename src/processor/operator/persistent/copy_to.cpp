#include "processor/operator/persistent/copy_to.h"

namespace kuzu {
namespace processor {

void CopyTo::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState.copyFuncLocalState = info->copyFunc.copyToInitLocal(*context->clientContext,
        *info->copyFuncBindData, info->isFlatVec);
    localState.inputVectors.reserve(info->inputVectorPoses.size());
    for (auto& inputVectorPos : info->inputVectorPoses) {
        localState.inputVectors.push_back(resultSet->getValueVector(inputVectorPos));
    }
}

void CopyTo::finalize(ExecutionContext* /*context*/) {
    info->copyFunc.copyToFinalize(*sharedState);
}

void CopyTo::executeInternal(processor::ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        info->copyFunc.copyToSink(*sharedState, *localState.copyFuncLocalState,
            *info->copyFuncBindData, localState.inputVectors);
    }
    info->copyFunc.copyToCombine(*sharedState, *localState.copyFuncLocalState);
}

bool CopyTo::isParallel() const {
    return info->copyFuncBindData->canParallel;
}

} // namespace processor
} // namespace kuzu
