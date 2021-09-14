#include "src/processor/include/physical_plan/operator/multiplicity_reducer/multiplicity_reducer.h"

namespace graphflow {
namespace processor {

void MultiplicityReducer::reInitialize() {
    PhysicalOperator::reInitialize();
    prevMultiplicity = 1;
    numRepeat = 0;
}

void MultiplicityReducer::getNextTuples() {
    metrics->executionTime.start();
    if (numRepeat == 0) {
        restoreMultiplicity();
        prevOperator->getNextTuples();
        if (resultSet->getNumTuples() == 0) {
            return;
        }
        saveMultiplicity();
        resultSet->multiplicity = 1;
    }
    numRepeat++;
    if (numRepeat == prevMultiplicity) {
        numRepeat = 0;
    }
    metrics->executionTime.stop();
}

} // namespace processor
} // namespace graphflow
