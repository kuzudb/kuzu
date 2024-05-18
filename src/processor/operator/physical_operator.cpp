#include "processor/operator/physical_operator.h"

#include "common/exception/interrupt.h"
#include "common/exception/runtime.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {
// LCOV_EXCL_START
std::string PhysicalOperatorUtils::operatorTypeToString(PhysicalOperatorType operatorType) {
    switch (operatorType) {
    case PhysicalOperatorType::ALTER:
        return "ALTER";
    case PhysicalOperatorType::AGGREGATE:
        return "AGGREGATE";
    case PhysicalOperatorType::AGGREGATE_SCAN:
        return "AGGREGATE_SCAN";
    case PhysicalOperatorType::ATTACH_DATABASE:
        return "ATTACH_DATABASE";
    case PhysicalOperatorType::BATCH_INSERT:
        return "BATCH_INSERT";
    case PhysicalOperatorType::COMMENT_ON:
        return "COMMENT_ON";
    case PhysicalOperatorType::COPY_RDF:
        return "COPY_RDF";
    case PhysicalOperatorType::COPY_TO:
        return "COPY_TO";
    case PhysicalOperatorType::CREATE_MACRO:
        return "CREATE_MACRO";
    case PhysicalOperatorType::CREATE_TABLE:
        return "CREATE_TABLE";
    case PhysicalOperatorType::CROSS_PRODUCT:
        return "CROSS_PRODUCT";
    case PhysicalOperatorType::DETACH_DATABASE:
        return "DETACH_DATABASE";
    case PhysicalOperatorType::DELETE_:
        return "DELETE";
    case PhysicalOperatorType::DROP_TABLE:
        return "DROP_TABLE";
    case PhysicalOperatorType::EMPTY_RESULT:
        return "EMPTY_RESULT";
    case PhysicalOperatorType::EXPORT_DATABASE:
        return "EXPORT_DATABASE";
    case PhysicalOperatorType::FILTER:
        return "FILTER";
    case PhysicalOperatorType::FLATTEN:
        return "FLATTEN";
    case PhysicalOperatorType::HASH_JOIN_BUILD:
        return "HASH_JOIN_BUILD";
    case PhysicalOperatorType::HASH_JOIN_PROBE:
        return "HASH_JOIN_PROBE";
    case PhysicalOperatorType::IMPORT_DATABASE:
        return "IMPORT_DATABASE";
    case PhysicalOperatorType::IN_QUERY_CALL:
        return "IN_QUERY_CALL";
    case PhysicalOperatorType::INDEX_LOOKUP:
        return "INDEX_LOOKUP";
    case PhysicalOperatorType::INDEX_SCAN:
        return "INDEX_SCAN";
    case PhysicalOperatorType::INSERT:
        return "INSERT";
    case PhysicalOperatorType::INTERSECT_BUILD:
        return "INTERSECT_BUILD";
    case PhysicalOperatorType::INTERSECT:
        return "INTERSECT";
    case PhysicalOperatorType::INSTALL_EXTENSION:
        return "INSTALL_EXTENSION";
    case PhysicalOperatorType::LIMIT:
        return "LIMIT";
    case PhysicalOperatorType::LOAD_EXTENSION:
        return "LOAD EXTENSION";
    case PhysicalOperatorType::MERGE:
        return "MERGE";
    case PhysicalOperatorType::MULTIPLICITY_REDUCER:
        return "MULTIPLICITY_REDUCER";
    case PhysicalOperatorType::PARTITIONER:
        return "PARTITIONER";
    case PhysicalOperatorType::PATH_PROPERTY_PROBE:
        return "PATH_PROPERTY_PROBE";
    case PhysicalOperatorType::PROJECTION:
        return "PROJECTION";
    case PhysicalOperatorType::PROFILE:
        return "PROFILE";
    case PhysicalOperatorType::RECURSIVE_JOIN:
        return "RECURSIVE_JOIN";
    case PhysicalOperatorType::RENAME_PROPERTY:
        return "RENAME_PROPERTY";
    case PhysicalOperatorType::RENAME_TABLE:
        return "RENAME_TABLE";
    case PhysicalOperatorType::RESULT_COLLECTOR:
        return "RESULT_COLLECTOR";
    case PhysicalOperatorType::SCAN_FRONTIER:
        return "SCAN_FRONTIER";
    case PhysicalOperatorType::SCAN_MULTI_NODE_TABLES:
        return "SCAN_MULTI_NODE_TABLES";
    case PhysicalOperatorType::SCAN_MULTI_REL_TABLES:
        return "SCAN_MULTI_REL_TABLES";
    case PhysicalOperatorType::SCAN_NODE_ID:
        return "SCAN_NODE_ID";
    case PhysicalOperatorType::SCAN_NODE_TABLE:
        return "SCAN_NODE_TABLE";
    case PhysicalOperatorType::SCAN_REL_TABLE:
        return "SCAN_REL_TABLE";
    case PhysicalOperatorType::SEMI_MASKER:
        return "SEMI_MASKER";
    case PhysicalOperatorType::SET_NODE_PROPERTY:
        return "SET_NODE_PROPERTY";
    case PhysicalOperatorType::SET_REL_PROPERTY:
        return "SET_REL_PROPERTY";
    case PhysicalOperatorType::SKIP:
        return "SKIP";
    case PhysicalOperatorType::STANDALONE_CALL:
        return "STANDALONE_CALL";
    case PhysicalOperatorType::TOP_K:
        return "TOP_K";
    case PhysicalOperatorType::TOP_K_SCAN:
        return "TOP_K_SCAN";
    case PhysicalOperatorType::TRANSACTION:
        return "TRANSACTION";
    case PhysicalOperatorType::ORDER_BY:
        return "ORDER_BY";
    case PhysicalOperatorType::ORDER_BY_MERGE:
        return "ORDER_BY_MERGE";
    case PhysicalOperatorType::ORDER_BY_SCAN:
        return "ORDER_BY_SCAN";
    case PhysicalOperatorType::UNION_ALL_SCAN:
        return "UNION_ALL_SCAN";
    case PhysicalOperatorType::UNWIND:
        return "UNWIND";
    case PhysicalOperatorType::USE_DATABASE:
        return "USE_DATABASE";
    default:
        throw RuntimeException("Unknown physical operator type.");
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

PhysicalOperator::PhysicalOperator(PhysicalOperatorType operatorType, physical_op_vector_t children,
    uint32_t id, const std::string& paramsString)
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
    if (!isSource()) {
        children[0]->initGlobalState(context);
    }
    initGlobalStateInternal(context);
}

void PhysicalOperator::initLocalState(ResultSet* resultSet_, ExecutionContext* context) {
    if (!isSource()) {
        children[0]->initLocalState(resultSet_, context);
    }
    resultSet = resultSet_;
    registerProfilingMetrics(context->profiler);
    initLocalStateInternal(resultSet_, context);
}

bool PhysicalOperator::getNextTuple(ExecutionContext* context) {
    if (context->clientContext->interrupted()) {
        throw InterruptException{};
    }
    metrics->executionTime.start();
    auto result = getNextTuplesInternal(context);
    context->clientContext->getProgressBar()->updateProgress(getProgress(context));
    metrics->executionTime.stop();
    return result;
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
        result.emplace_back(key + ": " + std::move(val));
    }
    return result;
}

double PhysicalOperator::getProgress(ExecutionContext* /*context*/) const {
    return 0;
}

} // namespace processor
} // namespace kuzu
