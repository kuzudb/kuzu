#pragma once

#include "src/processor/include/execution_context.h"
#include "src/processor/include/physical_plan/data_pos.h"
#include "src/processor/include/physical_plan/result/result_set.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"

namespace graphflow {
namespace processor {

// Sort in alphabetical order
enum PhysicalOperatorType : uint8_t {
    AGGREGATE,
    AGGREGATE_SCAN,
    COLUMN_EXTEND,
    COPY_NODE_CSV,
    COPY_REL_CSV,
    CREATE,
    CREATE_NODE_TABLE,
    CREATE_REL_TABLE,
    DELETE,
    DROP_TABLE,
    EXISTS,
    FACTORIZED_TABLE_SCAN,
    FILTER,
    FLATTEN,
    HASH_JOIN_BUILD,
    HASH_JOIN_PROBE,
    INTERSECT,
    LIMIT,
    LIST_EXTEND,
    MULTIPLICITY_REDUCER,
    PROJECTION,
    READ_REL_PROPERTY,
    RESULT_COLLECTOR,
    SCAN_NODE_ID,
    SCAN_STRUCTURED_PROPERTY,
    SCAN_UNSTRUCTURED_PROPERTY,
    SEMI_MASKER,
    SET_STRUCTURED_NODE_PROPERTY,
    SET_UNSTRUCTURED_NODE_PROPERTY,
    SKIP,
    ORDER_BY,
    ORDER_BY_MERGE,
    ORDER_BY_SCAN,
    UNION_ALL_SCAN,
    UNWIND,
    VAR_LENGTH_ADJ_LIST_EXTEND,
    VAR_LENGTH_COLUMN_EXTEND,
};

const string PhysicalOperatorTypeNames[] = {"AGGREGATE", "AGGREGATE_SCAN", "COLUMN_EXTEND",
    "COPY_NODE_CSV", "COPY_REL_CSV", "CREATE", "CREATE_NODE_TABLE", "CREATE_REL_TABLE", "DELETE",
    "DROP_TABLE", "EXISTS", "FACTORIZED_TABLE_SCAN", "FILTER", "FLATTEN", "HASH_JOIN_BUILD",
    "HASH_JOIN_PROBE", "INTERSECT", "LIMIT", "LIST_EXTEND", "MULTIPLICITY_REDUCER", "PROJECTION",
    "READ_REL_PROPERTY", "RESULT_COLLECTOR", "SCAN_NODE_ID", "SCAN_STRUCTURED_PROPERTY",
    "SCAN_UNSTRUCTURED_PROPERTY", "SEMI_MASKER", "SET_STRUCTURED_NODE_PROPERTY",
    "SET_UNSTRUCTURED_NODE_PROPERTY", "SKIP", "ORDER_BY", "ORDER_BY_MERGE", "ORDER_BY_SCAN",
    "UNION_ALL_SCAN", "UNWIND"};

struct OperatorMetrics {

public:
    OperatorMetrics(TimeMetric& executionTime, NumericMetric& numOutputTuple)
        : executionTime{executionTime}, numOutputTuple{numOutputTuple} {}

public:
    TimeMetric& executionTime;
    NumericMetric& numOutputTuple;
};

class PhysicalOperator {

public:
    // Leaf operator
    PhysicalOperator(uint32_t id, string paramsString)
        : id{id}, transaction{nullptr}, paramsString{std::move(paramsString)} {}
    // Unary operator
    PhysicalOperator(unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString);
    // Binary operator
    PhysicalOperator(unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
        uint32_t id, const string& paramsString);
    // This constructor is used by UnionAllScan only since it may have multiple children.
    PhysicalOperator(
        vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString);

    virtual ~PhysicalOperator() = default;

    inline uint32_t getOperatorID() const { return id; }

    inline void addChild(unique_ptr<PhysicalOperator> op) { children.push_back(std::move(op)); }
    inline PhysicalOperator* getChild(uint64_t idx) const { return children[idx].get(); }
    inline uint64_t getNumChildren() const { return children.size(); }
    unique_ptr<PhysicalOperator> moveUnaryChild();

    // This function grab leaf operator by traversing prevOperator pointer. For binary operators,
    // f.g. hash join probe, left nested loop join, caller should determine which branch's leaf to
    // get.
    PhysicalOperator* getLeafOperator();

    virtual PhysicalOperatorType getOperatorType() = 0;

    virtual shared_ptr<ResultSet> init(ExecutionContext* context);

    // Return false if no more tuples to pull, otherwise return true
    virtual bool getNextTuples() = 0;

    virtual unique_ptr<PhysicalOperator> clone() = 0;

    virtual void printMetricsToJson(nlohmann::json& json, Profiler& profiler);

    virtual double getExecutionTime(Profiler& profiler) const;

    inline uint64_t getNumOutputTuples(Profiler& profiler) const {
        return profiler.sumAllNumericMetricsWithKey(getNumTupleMetricKey());
    }

    vector<string> getAttributes(Profiler& profiler) const;
    inline string getTimeMetricKey() const { return "time-" + to_string(id); }

    inline string getParamsString() const { return paramsString; }

protected:
    inline string getNumTupleMetricKey() const { return "numTuple-" + to_string(id); }

    void registerProfilingMetrics(Profiler* profiler);

    void printTimeAndNumOutputMetrics(nlohmann::json& json, Profiler& profiler);

protected:
    uint32_t id;
    unique_ptr<OperatorMetrics> metrics;

    vector<unique_ptr<PhysicalOperator>> children;
    shared_ptr<ResultSet> resultSet;
    Transaction* transaction;

    string paramsString;
};

} // namespace processor
} // namespace graphflow
