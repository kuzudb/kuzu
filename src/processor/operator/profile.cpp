#include "processor/operator/profile.h"

#include <memory>

#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"
#include "main/plan_printer.h"
#include "processor/execution_context.h"
#include "processor/result/result_set.h"

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
