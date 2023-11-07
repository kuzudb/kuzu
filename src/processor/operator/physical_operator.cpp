#include "processor/operator/physical_operator.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {
// LCOV_EXCL_START
std::string PhysicalOperatorUtils::operatorTypeToString(PhysicalOperatorType operatorType) {
    switch (operatorType) {
    case PhysicalOperatorType::ADD_PROPERTY: {
        return "ADD_PROPERTY";
    }
    case PhysicalOperatorType::AGGREGATE: {
        return "AGGREGATE";
    }
    case PhysicalOperatorType::AGGREGATE_SCAN: {
        return "AGGREGATE_SCAN";
    }
    case PhysicalOperatorType::STANDALONE_CALL: {
        return "STANDALONE_CALL";
    }
    case PhysicalOperatorType::COPY_TO_CSV: {
        return "COPY_TO_CSV";
    }
    case PhysicalOperatorType::COPY_TO_PARQUET: {
        return "COPY_TO_PARQUET";
    }
    case PhysicalOperatorType::COPY_NODE: {
        return "COPY_NODE";
    }
    case PhysicalOperatorType::COPY_RDF: {
        return "COPY_RDF";
    }
    case PhysicalOperatorType::COPY_REL: {
        return "COPY_REL";
    }
    case PhysicalOperatorType::CREATE_MACRO: {
        return "CREATE_MACRO";
    }
    case PhysicalOperatorType::READER: {
        return "READER";
    }
    case PhysicalOperatorType::INSERT_NODE: {
        return "INSERT_NODE";
    }
    case PhysicalOperatorType::CREATE_NODE_TABLE: {
        return "CREATE_NODE_TABLE";
    }
    case PhysicalOperatorType::INSERT_REL: {
        return "INSERT_REL";
    }
    case PhysicalOperatorType::CREATE_REL_TABLE: {
        return "CREATE_REL_TABLE";
    }
    case PhysicalOperatorType::CREATE_RDF_GRAPH: {
        return "CREATE_RDF_TABLE";
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
    case PhysicalOperatorType::DROP_PROPERTY: {
        return "DROP_PROPERTY";
    }
    case PhysicalOperatorType::DROP_TABLE: {
        return "DROP_TABLE";
    }
    case PhysicalOperatorType::FACTORIZED_TABLE_SCAN: {
        return "FACTORIZED_TABLE_SCAN";
    }
    case PhysicalOperatorType::FILL_TABLE_ID: {
        return "FILL_TABLE_ID";
    }
    case PhysicalOperatorType::FILTER: {
        return "FILTER";
    }
    case PhysicalOperatorType::FLATTEN: {
        return "FLATTEN";
    }
    case PhysicalOperatorType::HASH_JOIN_BUILD: {
        return "HASH_JOIN_BUILD";
    }
    case PhysicalOperatorType::HASH_JOIN_PROBE: {
        return "HASH_JOIN_PROBE";
    }
    case PhysicalOperatorType::INDEX_LOOKUP: {
        return "INDEX_LOOKUP";
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
    case PhysicalOperatorType::MERGE: {
        return "MERGE";
    }
    case PhysicalOperatorType::MULTIPLICITY_REDUCER: {
        return "MULTIPLICITY_REDUCER";
    }
    case PhysicalOperatorType::PARTITIONER: {
        return "PARTITIONER";
    }
    case PhysicalOperatorType::PATH_PROPERTY_PROBE: {
        return "PATH_PROPERTY_PROBE";
    }
    case PhysicalOperatorType::PROJECTION: {
        return "PROJECTION";
    }
    case PhysicalOperatorType::RECURSIVE_JOIN: {
        return "RECURSIVE_JOIN";
    }
    case PhysicalOperatorType::RENAME_PROPERTY: {
        return "RENAME_PROPERTY";
    }
    case PhysicalOperatorType::RENAME_TABLE: {
        return "RENAME_TABLE";
    }
    case PhysicalOperatorType::RESULT_COLLECTOR: {
        return "RESULT_COLLECTOR";
    }
    case PhysicalOperatorType::SCAN_FRONTIER: {
        return "SCAN_FRONTIER";
    }
    case PhysicalOperatorType::SCAN_MULTI_NODE_TABLES: {
        return "SCAN_MULTI_NODE_TABLES";
    }
    case PhysicalOperatorType::SCAN_MULTI_REL_TABLES: {
        return "SCAN_MULTI_REL_TABLES";
    }
    case PhysicalOperatorType::SCAN_NODE_ID: {
        return "SCAN_NODE_ID";
    }
    case PhysicalOperatorType::SCAN_NODE_TABLE: {
        return "SCAN_NODE_TABLE";
    }
    case PhysicalOperatorType::SCAN_REL_TABLE: {
        return "SCAN_REL_TABLE";
    }
    case PhysicalOperatorType::SEMI_MASKER: {
        return "SEMI_MASKER";
    }
    case PhysicalOperatorType::SET_NODE_PROPERTY: {
        return "SET_NODE_PROPERTY";
    }
    case PhysicalOperatorType::SET_REL_PROPERTY: {
        return "SET_REL_PROPERTY";
    }
    case PhysicalOperatorType::SKIP: {
        return "SKIP";
    }
    case PhysicalOperatorType::TOP_K: {
        return "TOP_K";
    }
    case PhysicalOperatorType::TOP_K_SCAN: {
        return "TOP_K_SCAN";
    }
    case PhysicalOperatorType::TRANSACTION: {
        return "TRANSACTION";
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
    case PhysicalOperatorType::IN_QUERY_CALL: {
        return "IN_QUERY_CALL";
    }
    case PhysicalOperatorType::PROFILE: {
        return "PROFILE";
    }
    default:
        KU_UNREACHABLE;
    }
}
// LCOV_EXCL_STOP

PhysicalOperator::PhysicalOperator(PhysicalOperatorType operatorType,
    std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
    : PhysicalOperator{operatorType, id, paramsString} {
    children.push_back(std::move(child));
}

PhysicalOperator::PhysicalOperator(PhysicalOperatorType operatorType,
    std::unique_ptr<PhysicalOperator> left, std::unique_ptr<PhysicalOperator> right, uint32_t id,
    const std::string& paramsString)
    : PhysicalOperator{operatorType, id, paramsString} {
    children.push_back(std::move(left));
    children.push_back(std::move(right));
}

PhysicalOperator::PhysicalOperator(PhysicalOperatorType operatorType,
    std::vector<std::unique_ptr<PhysicalOperator>> children, uint32_t id,
    const std::string& paramsString)
    : PhysicalOperator{operatorType, id, paramsString} {
    for (auto& child : children) {
        this->children.push_back(std::move(child));
    }
}

std::unique_ptr<PhysicalOperator> PhysicalOperator::moveUnaryChild() {
    KU_ASSERT(children.size() == 1);
    auto result = std::move(children[0]);
    children.clear();
    return result;
}

void PhysicalOperator::initGlobalState(ExecutionContext* context) {
    // Init from right to left so that we init in the same order as we decompose.
    // TODO(Xiyang): this is a very implicit assumption. We should init global state during
    // decomposition ideally.
    for (auto i = children.size(); i > 0; --i) {
        children[i - 1]->initGlobalState(context);
    }
    initGlobalStateInternal(context);
}

void PhysicalOperator::initLocalState(ResultSet* resultSet_, ExecutionContext* context) {
    if (!isSource()) {
        children[0]->initLocalState(resultSet_, context);
    }
    transaction = context->clientContext->getActiveTransaction();
    resultSet = resultSet_;
    registerProfilingMetrics(context->profiler);
    initLocalStateInternal(resultSet_, context);
}

void PhysicalOperator::registerProfilingMetrics(Profiler* profiler) {
    auto executionTime = profiler->registerTimeMetric(getTimeMetricKey());
    auto numOutputTuple = profiler->registerNumericMetric(getNumTupleMetricKey());
    metrics = std::make_unique<OperatorMetrics>(*executionTime, *numOutputTuple);
}

double PhysicalOperator::getExecutionTime(Profiler& profiler) const {
    auto executionTime = profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    if (!isSource()) {
        executionTime -= profiler.sumAllTimeMetricsWithKey(children[0]->getTimeMetricKey());
    }
    return executionTime;
}

uint64_t PhysicalOperator::getNumOutputTuples(Profiler& profiler) const {
    return profiler.sumAllNumericMetricsWithKey(getNumTupleMetricKey());
}

std::unordered_map<std::string, std::string> PhysicalOperator::getProfilerKeyValAttributes(
    Profiler& profiler) const {
    std::unordered_map<std::string, std::string> result;
    result.insert({"ExecutionTime", std::to_string(getExecutionTime(profiler))});
    result.insert({"NumOutputTuples", std::to_string(getNumOutputTuples(profiler))});
    return result;
}

std::vector<std::string> PhysicalOperator::getProfilerAttributes(Profiler& profiler) const {
    std::vector<std::string> result;
    for (auto& [key, val] : getProfilerKeyValAttributes(profiler)) {
        result.emplace_back(key + ": " + val);
    }
    return result;
}

} // namespace processor
} // namespace kuzu
