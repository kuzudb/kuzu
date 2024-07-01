#include "processor/operator/persistent/copy_to.h"

namespace kuzu {
namespace processor {

void CopyTo::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState.exportFuncLocalState =
        info.exportFunc.initLocal(*context->clientContext, *info.bindData, info.isFlatVec);
    localState.inputVectors.reserve(info.inputVectorPoses.size());
    for (auto& inputVectorPos : info.inputVectorPoses) {
        localState.inputVectors.push_back(resultSet->getValueVector(inputVectorPos));
    }
}

void CopyTo::finalize(ExecutionContext* /*context*/) {
    info.exportFunc.finalize(*sharedState);
}

void CopyTo::executeInternal(processor::ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        info.exportFunc.sink(*sharedState, *localState.exportFuncLocalState, *info.bindData,
            localState.inputVectors);
    }
    info.exportFunc.combine(*sharedState, *localState.exportFuncLocalState);
}

} // namespace processor
} // namespace kuzu
