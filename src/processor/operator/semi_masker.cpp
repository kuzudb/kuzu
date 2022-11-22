#include "processor/operator/semi_masker.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> SemiMasker::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    scanNodeIDSharedState->initSemiMask(context->transaction);
    keyValueVector = resultSet->getValueVector(keyDataPos);
    assert(keyValueVector->dataType.typeID == NODE_ID);
    return resultSet;
}

bool SemiMasker::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    auto startIdx = keyValueVector->state->isFlat() ? keyValueVector->state->currIdx : 0;
    auto numValues =
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize;
    for (auto i = 0u; i < numValues; i++) {
        auto pos = keyValueVector->state->selVector->selectedPositions[i + startIdx];
        scanNodeIDSharedState->getSemiMask()->setMask(
            keyValueVector->getValue<nodeID_t>(pos).offset, maskerIdx);
    }
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
