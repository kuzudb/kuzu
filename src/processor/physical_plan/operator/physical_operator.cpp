#include "src/processor/include/physical_plan/operator/physical_operator.h"

#include <iostream>

namespace graphflow {
namespace processor {

PhysicalOperator::PhysicalOperator(ExecutionContext& context, uint32_t id)
    : context{context}, id{id} {
    registerProfilingMetrics();
}

PhysicalOperator::PhysicalOperator(
    unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{context, id} {
    children.push_back(move(child));
}

PhysicalOperator::PhysicalOperator(unique_ptr<PhysicalOperator> left,
    unique_ptr<PhysicalOperator> right, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{context, id} {
    children.push_back(move(left));
    children.push_back(move(right));
}

PhysicalOperator::PhysicalOperator(
    vector<unique_ptr<PhysicalOperator>> children, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{context, id} {
    for (auto& child : children) {
        this->children.push_back(move(child));
    }
}

PhysicalOperator* PhysicalOperator::getLeafOperator() {
    PhysicalOperator* op = this;
    while (op->getNumChildren()) {
        op = op->getChild(0);
    }
    return op;
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
    if (getNumChildren()) {
        prevExecutionTime = profiler.sumAllTimeMetricsWithKey(getChild(0)->getTimeMetricKey());
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
