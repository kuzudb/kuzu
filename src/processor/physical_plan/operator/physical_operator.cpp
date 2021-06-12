#include "src/processor/include/physical_plan/operator/physical_operator.h"

#include <iostream>

namespace graphflow {
namespace processor {

static string getTimeMetricKey(uint32_t id) {
    return "time-" + to_string(id);
}
static string getNumTupleMetricKey(uint32_t id) {
    return "numTuple-" + to_string(id);
}

PhysicalOperator::PhysicalOperator(unique_ptr<PhysicalOperator> prevOperator,
    PhysicalOperatorType operatorType, bool isOutDataChunkFiltered, ExecutionContext& context,
    uint32_t id)
    : prevOperator{move(prevOperator)}, operatorType{operatorType},
      isOutDataChunkFiltered{isOutDataChunkFiltered}, context{context}, id{id} {
    executionTime = context.profiler.registerTimeMetric(getTimeMetricKey(id));
    numOutputTuple = context.profiler.registerNumericMetric(getNumTupleMetricKey(id));
}

nlohmann::json PhysicalOperator::toJson(Profiler& profiler) {
    auto json = nlohmann::json();
    json["physicalName"] = PhysicalOperatorTypeNames[operatorType];
    double prevExecutionTime = 0.0;
    if (prevOperator) {
        prevExecutionTime = profiler.sumAllTimeMetricsWithKey(getTimeMetricKey(prevOperator->id));
    }
    // Time metric measures execution time of the subplan under current operator (like a CDF).
    // By subtracting prevOperator runtime, we get the runtime of current operator
    json["executionTime"] =
        profiler.sumAllTimeMetricsWithKey(getTimeMetricKey(id)) - prevExecutionTime;
    json["numOutputTuples"] = profiler.sumAllNumericMetricsWithKey(getNumTupleMetricKey(id));
    if (prevOperator) {
        json["child"] = prevOperator->toJson(profiler);
    }
    return json;
}

} // namespace processor
} // namespace graphflow
