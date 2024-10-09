#include "processor/operator/profile.h"

#include "main/plan_printer.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Profile::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    outputVector = resultSet->getValueVector(outputPos).get();
}

bool Profile::getNextTuplesInternal(ExecutionContext* context) {
    if (localState.hasExecuted) {
        return false;
    }
    localState.hasExecuted = true;
    ku_string_t profileStr;
    const auto planInString =
        main::PlanPrinter::printPlanToOstream(info.physicalPlan, context->profiler).str();
    StringVector::addString(outputVector, profileStr, planInString.c_str(), planInString.length());
    auto& selVector = outputVector->state->getSelVectorUnsafe();
    selVector.setSelSize(1);
    outputVector->setValue<ku_string_t>(selVector[0], profileStr);
    metrics->numOutputTuple.incrementByOne();
    return true;
}

} // namespace processor
} // namespace kuzu
