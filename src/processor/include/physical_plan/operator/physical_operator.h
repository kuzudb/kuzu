#pragma once

#include "src/processor/include/execution_context.h"
#include "src/processor/include/physical_plan/data_pos.h"
#include "src/processor/include/physical_plan/result/result_set.h"
#include "src/storage/include/buffer_manager.h"

using namespace std;

namespace graphflow {
namespace processor {

// Sort in alphabetical order
enum PhysicalOperatorType : uint8_t {
    AGGREGATE,
    AGGREGATE_SCAN,
    COLUMN_EXTEND,
    EXISTS,
    FILTER,
    FLATTEN,
    FRONTIER_EXTEND,
    HASH_JOIN_BUILD,
    HASH_JOIN_PROBE,
    INTERSECT,
    LEFT_NESTED_LOOP_JOIN,
    LIMIT,
    LIST_EXTEND,
    MULTIPLICITY_REDUCER,
    PROJECTION,
    READ_REL_PROPERTY,
    RESULT_COLLECTOR,
    RESULT_SCAN,
    SCAN_NODE_ID,
    SCAN_STRUCTURED_PROPERTY,
    SCAN_UNSTRUCTURED_PROPERTY,
    SKIP,
    ORDER_BY,
    ORDER_BY_MERGE,
    ORDER_BY_SCAN,
    UNION_ALL_SCAN,
};

const string PhysicalOperatorTypeNames[] = {"AGGREGATE", "AGGREGATE_SCAN", "COLUMN_EXTEND",
    "EXISTS", "FILTER", "FLATTEN", "FRONTIER_EXTEND", "HASH_JOIN_BUILD", "HASH_JOIN_PROBE",
    "INTERSECT", "LEFT_NESTED_LOOP_JOIN", "LIMIT", "LIST_EXTEND", "MULTIPLICITY_REDUCER",
    "PROJECTION", "READ_REL_PROPERTY", "RESULT_COLLECTOR", "RESULT_SCAN", "SCAN_NODE_ID",
    "SCAN_STRUCTURED_PROPERTY", "SCAN_UNSTRUCTURED_PROPERTY", "SKIP", "ORDER_BY", "ORDER_BY_MERGE",
    "ORDER_BY_SCAN", "UNION_ALL_SCAN"};

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
    PhysicalOperator(ExecutionContext& context, uint32_t id);
    // Unary operator
    PhysicalOperator(unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id);
    // Binary opeartor
    PhysicalOperator(unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
        ExecutionContext& context, uint32_t id);
    // This constructor is used by UnionAllScan only since it may have multiple children.
    PhysicalOperator(
        vector<unique_ptr<PhysicalOperator>> children, ExecutionContext& context, uint32_t id);

    virtual ~PhysicalOperator() = default;

    inline uint32_t getOperatorID() const { return id; }

    inline PhysicalOperator* getChild(uint64_t idx) const { return children[idx].get(); }
    inline uint64_t getNumChildren() const { return children.size(); }

    // This function grab leaf operator by traversing prevOperator pointer. For binary operators,
    // f.g. hash join probe, left nested loop join, caller should determine which branch's leaf to
    // get.
    PhysicalOperator* getLeafOperator();

    virtual PhysicalOperatorType getOperatorType() = 0;

    virtual shared_ptr<ResultSet> initResultSet() = 0;

    // Only operators that can appear in a subPlan need to overwrite this function. Currently, we
    // allow the following operators in subPlan: resultSetScan, extend, scanProperty, flatten,
    // filter, intersect, projection, exists, leftNestedLoopJoin.
    virtual void reInitToRerunSubPlan() {
        throw invalid_argument("Operator " + PhysicalOperatorTypeNames[getOperatorType()] +
                               "  does not implement reInitToRerunSubPlan().");
    }

    // Return false if no more tuples to pull, otherwise return true
    virtual bool getNextTuples() = 0;

    shared_ptr<ResultSet> getResultSet() { return resultSet; };

    virtual unique_ptr<PhysicalOperator> clone() = 0;

    virtual void printMetricsToJson(nlohmann::json& json, Profiler& profiler);

    double getExecutionTime(Profiler& profiler) const;

    inline uint64_t getNumOutputTuples(Profiler& profiler) const {
        return profiler.sumAllNumericMetricsWithKey(getNumTupleMetricKey());
    }

    vector<string> getAttributes(Profiler& profiler) const;

protected:
    inline string getTimeMetricKey() const { return "time-" + to_string(id); }
    inline string getNumTupleMetricKey() const { return "numTuple-" + to_string(id); }

    void registerProfilingMetrics();

    void printTimeAndNumOutputMetrics(nlohmann::json& json, Profiler& profiler);

protected:
    ExecutionContext& context;
    uint32_t id;
    unique_ptr<OperatorMetrics> metrics;

    vector<unique_ptr<PhysicalOperator>> children;
    shared_ptr<ResultSet> resultSet;
};

} // namespace processor
} // namespace graphflow
