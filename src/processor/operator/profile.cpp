#include "processor/operator/profile.h"

#include "main/plan_printer.h"
#include "processor/execution_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void Profile::executeInternal(ExecutionContext* context) {
    const auto planInString =
        main::PlanPrinter::printPlanToOstream(info.physicalPlan, context->profiler).str();
    appendMessage(planInString, context->clientContext->getMemoryManager());
}

} // namespace processor
} // namespace kuzu
