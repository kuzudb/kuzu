#include "processor/operator/physical_operator.h"

#include <regex>

#include "common/exception.h"

namespace kuzu {
namespace processor {

std::string PhysicalOperatorUtils::operatorTypeToString(PhysicalOperatorType operatorType) {
    switch (operatorType) {
    case PhysicalOperatorType::AGGREGATE: {
        return "AGGREGATE";
    }
    case PhysicalOperatorType::AGGREGATE_SCAN: {
        return "AGGREGATE_SCAN";
    }
    case PhysicalOperatorType::COLUMN_EXTEND: {
        return "COLUMN_EXTEND";
    }
    case PhysicalOperatorType::COPY_NODE_CSV: {
        return "COPY_NODE_CSV";
    }
    case PhysicalOperatorType::COPY_REL_CSV: {
        return "COPY_REL_CSV";
    }
    case PhysicalOperatorType::CREATE_NODE: {
        return "CREATE_NODE";
    }
    case PhysicalOperatorType::CREATE_NODE_TABLE: {
        return "CREATE_NODE_TABLE";
    }
    case PhysicalOperatorType::CREATE_REL: {
        return "CREATE_REL";
    }
    case PhysicalOperatorType::CREATE_REL_TABLE: {
        return "CREATE_REL_TABLE";
    }
    case PhysicalOperatorType::CROSS_PRODUCT: {
        return "CROSS_PRODUCT";
    }
    case PhysicalOperatorType::DELETE_NODE: {
        return "DELETE_NODE";
    }
    case PhysicalOperatorType::DELETE_REL: {
        return "DELETE_REL";
    }
    case PhysicalOperatorType::DROP_TABLE: {
        return "DROP_TABLE";
    }
    case PhysicalOperatorType::FACTORIZED_TABLE_SCAN: {
        return "FACTORIZED_TABLE_SCAN";
    }
    case PhysicalOperatorType::FILTER: {
        return "FILTER";
    }
    case PhysicalOperatorType::FLATTEN: {
        return "FLATTEN";
    }
    case PhysicalOperatorType::GENERIC_EXTEND: {
        return "GENERIC_EXTEND";
    }
    case PhysicalOperatorType::HASH_JOIN_BUILD: {
        return "HASH_JOIN_BUILD";
    }
    case PhysicalOperatorType::HASH_JOIN_PROBE: {
        return "HASH_JOIN_PROBE";
    }
    case PhysicalOperatorType::INDEX_SCAN: {
        return "INDEX_SCAN";
    }
    case PhysicalOperatorType::INTERSECT_BUILD: {
        return "INTERSECT_BUILD";
    }
    case PhysicalOperatorType::INTERSECT: {
        return "INTERSECT";
    }
    case PhysicalOperatorType::LIMIT: {
        return "LIMIT";
    }
    case PhysicalOperatorType::LIST_EXTEND: {
        return "LIST_EXTEND";
    }
    case PhysicalOperatorType::MULTIPLICITY_REDUCER: {
        return "MULTIPLICITY_REDUCER";
    }
    case PhysicalOperatorType::PROJECTION: {
        return "PROJECTION";
    }
    case PhysicalOperatorType::SCAN_REL_PROPERTY: {
        return "SCAN_REL_PROPERTY";
    }
    case PhysicalOperatorType::RESULT_COLLECTOR: {
        return "RESULT_COLLECTOR";
    }
    case PhysicalOperatorType::SCAN_NODE_ID: {
        return "SCAN_NODE_ID";
    }
    case PhysicalOperatorType::SCAN_NODE_PROPERTY: {
        return "SCAN_NODE_PROPERTY";
    }
    case PhysicalOperatorType::SEMI_MASKER: {
        return "SEMI_MASKER";
    }
    case PhysicalOperatorType::SET_NODE_PROPERTY: {
        return "SET_NODE_PROPERTY";
    }
    case PhysicalOperatorType::SKIP: {
        return "SKIP";
    }
    case PhysicalOperatorType::ORDER_BY: {
        return "ORDER_BY";
    }
    case PhysicalOperatorType::ORDER_BY_MERGE: {
        return "ORDER_BY_MERGE";
    }
    case PhysicalOperatorType::ORDER_BY_SCAN: {
        return "ORDER_BY_SCAN";
    }
    case PhysicalOperatorType::UNION_ALL_SCAN: {
        return "UNION_ALL_SCAN";
    }
    case PhysicalOperatorType::UNWIND: {
        return "UNWIND";
    }
    case PhysicalOperatorType::VAR_LENGTH_ADJ_LIST_EXTEND: {
        return "VAR_LENGTH_ADJ_EXTEND";
    }
    case PhysicalOperatorType::VAR_LENGTH_COLUMN_EXTEND: {
        return "VAR_LENGTH_COL_EXTEND";
    }
    case PhysicalOperatorType::SHORTEST_PATH_ADJ_LIST: {
        return "SHORTEST_PATH_ADJ_LIST";
    }
    case PhysicalOperatorType::SHORTEST_PATH_ADJ_COL: {
        return "SHORTEST_PATH_ADJ_COL";
    }
    default:
        throw common::NotImplementedException("physicalOperatorTypeToString()");
    }
}

PhysicalOperator::PhysicalOperator(PhysicalOperatorType operatorType,
    unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
    : PhysicalOperator{operatorType, id, paramsString} {
    children.push_back(std::move(child));
}

PhysicalOperator::PhysicalOperator(PhysicalOperatorType operatorType,
    unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right, uint32_t id,
    const string& paramsString)
    : PhysicalOperator{operatorType, id, paramsString} {
    children.push_back(std::move(left));
    children.push_back(std::move(right));
}

PhysicalOperator::PhysicalOperator(PhysicalOperatorType operatorType,
    vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString)
    : PhysicalOperator{operatorType, id, paramsString} {
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
