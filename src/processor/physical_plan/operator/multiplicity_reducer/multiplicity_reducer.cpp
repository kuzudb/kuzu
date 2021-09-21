#include "src/processor/include/physical_plan/operator/multiplicity_reducer/multiplicity_reducer.h"

namespace graphflow {
namespace processor {

void MultiplicityReducer::reInitialize() {
    PhysicalOperator::reInitialize();
    prevMultiplicity = 1;
    numRepeat = 0;
}

bool MultiplicityReducer::getNextTuples() {
    metrics->executionTime.start();
    if (numRepeat == 0) {
        restoreMultiplicity();
        if (!prevOperator->getNextTuples()) {
            metrics->executionTime.stop();
            return false;
        }
        saveMultiplicity();
        resultSet->multiplicity = 1;
    }
    numRepeat++;
    if (numRepeat == prevMultiplicity) {
        numRepeat = 0;
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow
