#include "processor/operator/physical_operator.h"

#include <regex>

namespace kuzu {
namespace processor {

PhysicalOperator::PhysicalOperator(
    unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
    : PhysicalOperator{id, paramsString} {
    children.push_back(std::move(child));
}

PhysicalOperator::PhysicalOperator(unique_ptr<PhysicalOperator> left,
    unique_ptr<PhysicalOperator> right, uint32_t id, const string& paramsString)
    : PhysicalOperator{id, paramsString} {
    children.push_back(std::move(left));
    children.push_back(std::move(right));
}

PhysicalOperator::PhysicalOperator(
    vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString)
    : PhysicalOperator{id, paramsString} {
    for (auto& child : children) {
        this->children.push_back(std::move(child));
    }
}

unique_ptr<PhysicalOperator> PhysicalOperator::moveUnaryChild() {
    assert(children.size() == 1);
    auto result = std::move(children[0]);
    children.clear();
    return result;
}

void PhysicalOperator::initGlobalState(ExecutionContext* context) {
    for (auto& child : children) {
        child->initGlobalState(context);
    }
    initGlobalStateInternal(context);
}

void PhysicalOperator::initLocalState(ResultSet* _resultSet, ExecutionContext* context) {
    if (!children.empty()) {
        assert(children.size() == 1);
        children[0]->initLocalState(_resultSet, context);
    }
    transaction = context->transaction;
    resultSet = _resultSet;
    registerProfilingMetrics(context->profiler);
    initLocalStateInternal(_resultSet, context);
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
    vector<string> result;
    result.emplace_back("ExecutionTime: " + to_string(getExecutionTime(profiler)));
    result.emplace_back("NumOutputTuples: " + to_string(getNumOutputTuples(profiler)));
    return result;
}

} // namespace processor
} // namespace kuzu
