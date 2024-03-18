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
    auto planPrinter = std::make_unique<main::PlanPrinter>(info.physicalPlan, context->profiler);
    auto planInString = planPrinter->printPlanToOstream().str();
    StringVector::addString(outputVector, profileStr, planInString.c_str(), planInString.length());
    auto selVector = outputVector->state->selVector;
    selVector->selectedSize = 1;
    outputVector->setValue<ku_string_t>(selVector->selectedPositions[0], profileStr);
    metrics->numOutputTuple.increase(1);
    return true;
}

} // namespace processor
} // namespace kuzu
