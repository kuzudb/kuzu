#include "src/processor/include/physical_plan/operator/physical_operator.h"

#include <iostream>

namespace graphflow {
namespace processor {

PhysicalOperator::PhysicalOperator(unique_ptr<PhysicalOperator> prevOperator,
    PhysicalOperatorType operatorType, ExecutionContext& context, uint32_t id)
    : prevOperator{move(prevOperator)}, operatorType{operatorType}, context{context}, id{id} {
    registerProfilingMetrics();
}

PhysicalOperator* PhysicalOperator::getLeafOperator() {
    PhysicalOperator* op = this;
    while (op->prevOperator != nullptr) {
        op = op->prevOperator.get();
    }
    return op;
}

void PhysicalOperator::initResultSet(const shared_ptr<ResultSet>& resultSet) {
    if (prevOperator != nullptr) {
        prevOperator->initResultSet(resultSet);
    }
    this->resultSet = resultSet;
}

void PhysicalOperator::reInitialize() {
    if (prevOperator) {
        prevOperator->reInitialize();
    }
}

void PhysicalOperator::registerProfilingMetrics() {
    auto executionTime = context.profiler.registerTimeMetric(getTimeMetricKey());
    auto numOutputTuple = context.profiler.registerNumericMetric(getNumTupleMetricKey());
    auto numBufferHit = context.profiler.registerNumericMetric(getNumBufferHitMetricKey());
    auto numBufferMiss = context.profiler.registerNumericMetric(getNumBufferMissMetricKey());
    auto numIO = context.profiler.registerNumericMetric(getNumIOMetricKey());
    auto bufferManageMetrics =
        make_unique<BufferManagerMetrics>(*numBufferHit, *numBufferMiss, *numIO);
    metrics =
        make_unique<OperatorMetrics>(*executionTime, *numOutputTuple, move(bufferManageMetrics));
}

void PhysicalOperator::printMetricsToJson(nlohmann::json& json, Profiler& profiler) {
    printTimeAndNumOutputMetrics(json, profiler);
}

void PhysicalOperator::printTimeAndNumOutputMetrics(nlohmann::json& json, Profiler& profiler) {
    double prevExecutionTime = 0.0;
    if (prevOperator) {
        prevExecutionTime = profiler.sumAllTimeMetricsWithKey(prevOperator->getTimeMetricKey());
    }
    // Time metric measures execution time of the subplan under current operator (like a CDF).
    // By subtracting prevOperator runtime, we get the runtime of current operator
    json["executionTime"] =
        to_string(profiler.sumAllTimeMetricsWithKey(getTimeMetricKey()) - prevExecutionTime);
    json["numOutputTuples"] = profiler.sumAllNumericMetricsWithKey(getNumTupleMetricKey());
}

void PhysicalOperator::printBufferManagerMetrics(nlohmann::json& json, Profiler& profiler) {
    auto bufferHit = profiler.sumAllNumericMetricsWithKey(getNumBufferHitMetricKey());
    auto bufferMiss = profiler.sumAllNumericMetricsWithKey(getNumBufferMissMetricKey());
    json["numBufferHit"] = bufferHit;
    json["numBufferMiss"] = bufferMiss;
    json["cacheMissRatio"] = to_string(((double)bufferMiss) / (double)(bufferMiss + bufferHit));
    json["numIO"] = profiler.sumAllNumericMetricsWithKey(getNumIOMetricKey());
}

} // namespace processor
} // namespace graphflow
