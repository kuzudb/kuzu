#include "src/main/include/plan_printer.h"

#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

namespace graphflow {
namespace main {

nlohmann::json PlanPrinter::printPlanToJson(PhysicalPlan* physicalPlan, Profiler& profiler) {
    return toJson(physicalPlan->lastOperator.get(), profiler);
}

nlohmann::json PlanPrinter::toJson(PhysicalOperator* physicalOperator, Profiler& profiler) {
    auto json = nlohmann::json();
    auto operatorType = physicalOperator->operatorType;
    auto operatorID = physicalOperator->id;
    // flatten, result collector do not have corresponding logical operator
    auto operatorName = PhysicalOperatorTypeNames[operatorType];
    if (physicalToLogicalOperatorMap.contains(operatorID)) {
        operatorName +=
            "(" + physicalToLogicalOperatorMap.at(operatorID)->getExpressionsForPrinting() + ")";
    }
    json["name"] = operatorName;
    if (operatorType == HASH_JOIN_PROBE) {
        json["buildChild"] =
            toJson(((HashJoinProbe<true>*)physicalOperator)->buildSidePrevOp.get(), profiler);
    }
    if (physicalOperator->prevOperator) {
        json["child"] = toJson(physicalOperator->prevOperator.get(), profiler);
    }
    if (profiler.enabled) {
        physicalOperator->printMetricsToJson(json, profiler);
    }
    return json;
}

} // namespace main
} // namespace graphflow
