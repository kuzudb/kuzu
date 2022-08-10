#include "src/processor/include/physical_plan/operator/semi_masker.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> SemiMasker::init(graphflow::processor::ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
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
    auto values = (nodeID_t*)keyValueVector->values;
    if (keyValueVector->state->isFlat()) {
        auto pos = keyValueVector->state->getPositionOfCurrIdx();
        scanNodeIDSharedState->setMask(values[pos].offset);
    } else {
        for (auto i = 0u; i < keyValueVector->state->selVector->selectedSize; i++) {
            auto pos = keyValueVector->state->selVector->selectedPositions[i];
            scanNodeIDSharedState->setMask(values[pos].offset);
        }
    }
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(
        keyValueVector->state->isFlat() ? 1 : keyValueVector->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace graphflow
