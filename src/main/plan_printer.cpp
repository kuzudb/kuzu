#include "src/main/include/plan_printer.h"

namespace graphflow {
namespace main {

nlohmann::json PlanPrinter::printPlanToJson(Profiler& profiler) {
    return toJson(physicalPlan->lastOperator.get(), profiler);
}

nlohmann::json PlanPrinter::toJson(PhysicalOperator* physicalOperator, Profiler& profiler) {
    auto json = nlohmann::json();
    auto operatorType = physicalOperator->getOperatorType();
    auto operatorID = physicalOperator->getOperatorID();
    // flatten, result collector do not have corresponding logical operator
    auto operatorName = PhysicalOperatorTypeNames[operatorType];
    if (physicalToLogicalOperatorMap.contains(operatorID)) {
        operatorName +=
            "(" + physicalToLogicalOperatorMap.at(operatorID)->getExpressionsForPrinting() + ")";
    }
    json["name"] = operatorName;
    if (physicalOperator->hasFirstChild()) {
        json["prev"] = toJson(physicalOperator->getFirstChild(), profiler);
    }
    if (physicalOperator->hasSecondChild()) {
        json["right"] = toJson(physicalOperator->getSecondChild(), profiler);
    }
    if (profiler.enabled) {
        physicalOperator->printMetricsToJson(json, profiler);
    }
    return json;
}

} // namespace main
} // namespace graphflow
