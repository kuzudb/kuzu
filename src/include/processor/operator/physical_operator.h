#pragma once

#include "processor/execution_context.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

enum class PhysicalOperatorType : uint8_t {
    ALTER,
    AGGREGATE,
    AGGREGATE_SCAN,
    ATTACH_DATABASE,
    BATCH_INSERT,
    COMMENT_ON,
    COPY_RDF,
    COPY_TO,
    CREATE_MACRO,
    CREATE_SEQUENCE,
    CREATE_TABLE,
    CREATE_TYPE,
    CROSS_PRODUCT,
    DETACH_DATABASE,
    DELETE_,
    DROP_TABLE,
    EMPTY_RESULT,
    EXPORT_DATABASE,
    FILTER,
    FLATTEN,
    GDS_CALL,
    HASH_JOIN_BUILD,
    HASH_JOIN_PROBE,
    IMPORT_DATABASE,
    IN_QUERY_CALL,
    INDEX_LOOKUP,
    INDEX_SCAN,
    INSERT,
    INTERSECT_BUILD,
    INTERSECT,
    INSTALL_EXTENSION,
    LIMIT,
    LOAD_EXTENSION,
    MERGE,
    MULTIPLICITY_REDUCER,
    OFFSET_SCAN_NODE_TABLE,
    PARTITIONER,
    PATH_PROPERTY_PROBE,
    PRIMARY_KEY_SCAN_NODE_TABLE,
    PROJECTION,
    PROFILE,
    RECURSIVE_JOIN,
    RENAME_PROPERTY,
    RENAME_TABLE,
    RESULT_COLLECTOR,
    SCAN_NODE_TABLE,
    SCAN_REL_TABLE,
    SEMI_MASKER,
    SET_PROPERTY,
    SKIP,
    STANDALONE_CALL,
    TOP_K,
    TOP_K_SCAN,
    TRANSACTION,
    ORDER_BY,
    ORDER_BY_MERGE,
    ORDER_BY_SCAN,
    UNION_ALL_SCAN,
    UNWIND,
    USE_DATABASE,
};

class PhysicalOperatorUtils {
public:
    static std::string operatorTypeToString(PhysicalOperatorType operatorType);
};

struct OperatorMetrics {

public:
    OperatorMetrics(common::TimeMetric& executionTime, common::NumericMetric& numOutputTuple)
        : executionTime{executionTime}, numOutputTuple{numOutputTuple} {}

public:
    common::TimeMetric& executionTime;
    common::NumericMetric& numOutputTuple;
};

class PhysicalOperator;
using physical_op_vector_t = std::vector<std::unique_ptr<PhysicalOperator>>;

class PhysicalOperator {
public:
    // Leaf operator
    PhysicalOperator(PhysicalOperatorType operatorType, uint32_t id, std::string paramsString)
        : id{id}, operatorType{operatorType}, paramsString{std::move(paramsString)} {}
    // Unary operator
    PhysicalOperator(PhysicalOperatorType operatorType, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString);
    // Binary operator
    PhysicalOperator(PhysicalOperatorType operatorType, std::unique_ptr<PhysicalOperator> left,
        std::unique_ptr<PhysicalOperator> right, uint32_t id, const std::string& paramsString);
    // This constructor is used by UnionAllScan only since it may have multiple children.
    PhysicalOperator(PhysicalOperatorType operatorType, physical_op_vector_t children, uint32_t id,
        const std::string& paramsString);

    virtual ~PhysicalOperator() = default;

    uint32_t getOperatorID() const { return id; }

    PhysicalOperatorType getOperatorType() const { return operatorType; }

    virtual bool isSource() const { return false; }
    virtual bool isSink() const { return false; }
    virtual bool isParallel() const { return true; }

    void addChild(std::unique_ptr<PhysicalOperator> op) { children.push_back(std::move(op)); }
    PhysicalOperator* getChild(uint64_t idx) const { return children[idx].get(); }
    uint64_t getNumChildren() const { return children.size(); }
    std::unique_ptr<PhysicalOperator> moveUnaryChild();

    std::string getParamsString() const { return paramsString; }

    // Global state is initialized once.
    void initGlobalState(ExecutionContext* context);
    // Local state is initialized for each thread.
    void initLocalState(ResultSet* resultSet, ExecutionContext* context);

    bool getNextTuple(ExecutionContext* context);

    std::unordered_map<std::string, std::string> getProfilerKeyValAttributes(
        common::Profiler& profiler) const;
    std::vector<std::string> getProfilerAttributes(common::Profiler& profiler) const;

    virtual std::unique_ptr<PhysicalOperator> clone() = 0;

    virtual double getProgress(ExecutionContext* context) const;

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<PhysicalOperator*, TARGET*>(this);
    }

protected:
    virtual void initGlobalStateInternal(ExecutionContext* /*context*/) {}
    virtual void initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) {}
    // Return false if no more tuples to pull, otherwise return true
    virtual bool getNextTuplesInternal(ExecutionContext* context) = 0;

    std::string getTimeMetricKey() const { return "time-" + std::to_string(id); }
    std::string getNumTupleMetricKey() const { return "numTuple-" + std::to_string(id); }

    void registerProfilingMetrics(common::Profiler* profiler);

    double getExecutionTime(common::Profiler& profiler) const;
    uint64_t getNumOutputTuples(common::Profiler& profiler) const;

protected:
    uint32_t id;
    std::unique_ptr<OperatorMetrics> metrics;
    PhysicalOperatorType operatorType;

    physical_op_vector_t children;
    ResultSet* resultSet;

    std::string paramsString;
};

} // namespace processor
} // namespace kuzu
