#include "processor/operator/persistent/copy_to.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CopyTo::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->getWriter()->init();
}

void CopyTo::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    outputVectors.reserve(vectorsToCopyPos.size());
    for (auto& pos : vectorsToCopyPos) {
        outputVectors.push_back(resultSet->getValueVector(pos).get());
    }
}

void CopyTo::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {
        sharedState->getWriter()->writeValues(outputVectors);
    }
}

void CopyTo::finalize(ExecutionContext* context) {
    sharedState->getWriter()->closeFile();
}

} // namespace processor
} // namespace kuzu
