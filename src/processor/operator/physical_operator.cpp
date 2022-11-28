#include "processor/operator/physical_operator.h"

#include <regex>

namespace kuzu {
namespace processor {

PhysicalOperator::PhysicalOperator(
    unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
    : PhysicalOperator{id, paramsString} {
    children.push_back(move(child));
}

PhysicalOperator::PhysicalOperator(unique_ptr<PhysicalOperator> left,
    unique_ptr<PhysicalOperator> right, uint32_t id, const string& paramsString)
    : PhysicalOperator{id, paramsString} {
    children.push_back(move(left));
    children.push_back(move(right));
}

PhysicalOperator::PhysicalOperator(
    vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString)
    : PhysicalOperator{id, paramsString} {
    for (auto& child : children) {
        this->children.push_back(move(child));
    }
}

unique_ptr<PhysicalOperator> PhysicalOperator::moveUnaryChild() {
    assert(children.size() == 1);
    auto result = std::move(children[0]);
    children.clear();
    return result;
}

shared_ptr<ResultSet> PhysicalOperator::init(ExecutionContext* context) {
    transaction = context->transaction;
    registerProfilingMetrics(context->profiler);
    if (!children.empty()) {
        resultSet = children[0]->init(context);
    }
    return resultSet;
}

void PhysicalOperator::initGlobalState(ExecutionContext* context) {
    for (auto& child : children) {
        child->initGlobalState(context);
    }
    initGlobalStateInternal(context);
}

void PhysicalOperator::registerProfilingMetrics(Profiler* profiler) {
    auto executionTime = profiler->registerTimeMetric(getTimeMetricKey());
    auto numOutputTuple = profiler->registerNumericMetric(getNumTupleMetricKey());

    metrics = make_unique<OperatorMetrics>(*executionTime, *numOutputTuple);
}

void PhysicalOperator::printMetricsToJson(nlohmann::json& json, Profiler& profiler) {
    printTimeAndNumOutputMetrics(json, profiler);
}

void PhysicalOperator::printTimeAndNumOutputMetrics(nlohmann::json& json, Profiler& profiler) {
    double prevExecutionTime = 0.0;
    if (getNumChildren()) {
        prevExecutionTime = profiler.sumAllTimeMetricsWithKey(children[0]->getTimeMetricKey());
    }
    // Time metric measures execution time of the subplan under current operator (like a CDF).
    // By subtracting prevOperator runtime, we get the runtime of current operator
    auto executionTime = profiler.sumAllTimeMetricsWithKey(getTimeMetricKey()) - prevExecutionTime;
    auto numOutputTuples = profiler.sumAllNumericMetricsWithKey(getNumTupleMetricKey());
    json["executionTime"] = to_string(executionTime);
    json["numOutputTuples"] = numOutputTuples;
}

double PhysicalOperator::getExecutionTime(Profiler& profiler) const {
    double prevExecutionTime = 0.0;
    for (auto i = 0u; i < getNumChildren(); i++) {
        prevExecutionTime += profiler.sumAllTimeMetricsWithKey(children[i]->getTimeMetricKey());
    }
    return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey()) - prevExecutionTime;
}

vector<string> PhysicalOperator::getAttributes(Profiler& profiler) const {
    vector<string> metrics;
    metrics.emplace_back("ExecutionTime: " + to_string(getExecutionTime(profiler)));
    metrics.emplace_back("NumOutputTuples: " + to_string(getNumOutputTuples(profiler)));
    return metrics;
}

} // namespace processor
} // namespace kuzu
