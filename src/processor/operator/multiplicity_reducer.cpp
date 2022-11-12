#include "include/multiplicity_reducer.h"

namespace kuzu {
namespace processor {

bool MultiplicityReducer::getNextTuples() {
    metrics->executionTime.start();
    if (numRepeat == 0) {
        restoreMultiplicity();
        if (!children[0]->getNextTuples()) {
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
} // namespace kuzu
