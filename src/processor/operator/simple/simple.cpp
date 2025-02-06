#include "processor/operator/simple/simple.h"

#include "common/metric.h"

namespace kuzu {
namespace processor {

void Simple::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    outputVector = resultSet->getValueVector(outputPos).get();
}

bool Simple::getNextTuplesInternal(ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
    executeInternal(context);
    outputVector->setValue<std::string>(outputVector->state->getSelVector()[0], getOutputMsg());
    metrics->numOutputTuple.incrementByOne();
    return true;
}

} // namespace processor
} // namespace kuzu
