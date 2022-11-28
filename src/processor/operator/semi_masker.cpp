#include "processor/operator/semi_masker.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> SemiMasker::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    keyValueVector = resultSet->getValueVector(keyDataPos);
    assert(keyValueVector->dataType.typeID == NODE_ID);
    return resultSet;
}

bool SemiMasker::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    auto startIdx = keyValueVector->state->isFlat() ? keyValueVector->state->currIdx : 0;
    auto numValues =
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize;
    for (auto i = 0u; i < numValues; i++) {
        auto pos = keyValueVector->state->selVector->selectedPositions[i + startIdx];
        scanTableNodeIDSharedState->getSemiMask()->setMask(
            keyValueVector->getValue<nodeID_t>(pos).offset, maskerIdx);
    }
    metrics->numOutputTuple.increase(
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize);
    return true;
}

void SemiMasker::initGlobalStateInternal(ExecutionContext* context) {
    scanTableNodeIDSharedState->initSemiMask(context->transaction);
}

} // namespace processor
} // namespace kuzu
