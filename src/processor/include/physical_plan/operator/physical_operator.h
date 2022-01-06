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
    AGGREGATION,
    AGGREGATION_SCAN,
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
};

const string PhysicalOperatorTypeNames[] = {"AGGREGATION", "AGGREGATION_SCAN", "COLUMN_EXTEND",
    "EXISTS", "FILTER", "FLATTEN", "FRONTIER_EXTEND", "HASH_JOIN_BUILD", "HASH_JOIN_PROBE",
    "INTERSECT", "LEFT_NESTED_LOOP_JOIN", "LIMIT", "LIST_EXTEND", "MULTIPLICITY_REDUCER",
    "PROJECTION", "READ_REL_PROPERTY", "RESULT_COLLECTOR", "RESULT_SCAN", "SCAN_NODE_ID",
    "SCAN_STRUCTURED_PROPERTY", "SCAN_UNSTRUCTURED_PROPERTY", "SKIP", "ORDER_BY", "ORDER_BY_MERGE",
    "ORDER_BY_SCAN"};

struct OperatorMetrics {

public:
    OperatorMetrics(TimeMetric& executionTime, NumericMetric& numOutputTuple,
        unique_ptr<BufferManagerMetrics> bufferManagerMetrics)
        : executionTime{executionTime}, numOutputTuple{numOutputTuple}, bufferManagerMetrics{move(
                                                                            bufferManagerMetrics)} {
    }

public:
    TimeMetric& executionTime;
    NumericMetric& numOutputTuple;
    unique_ptr<BufferManagerMetrics> bufferManagerMetrics;
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

    virtual ~PhysicalOperator() = default;

    inline uint32_t getOperatorID() const { return id; }

    inline bool hasFirstChild() const { return !children.empty(); }
    inline bool hasSecondChild() const { return children.size() == 2; }
    inline PhysicalOperator* getFirstChild() const { return children[0].get(); }
    inline PhysicalOperator* getSecondChild() const { return children[1].get(); }

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

protected:
    inline string getTimeMetricKey() const { return "time-" + to_string(id); }
    inline string getNumTupleMetricKey() const { return "numTuple-" + to_string(id); }
    inline string getNumBufferHitMetricKey() const { return "numBufferHit-" + to_string(id); }
    inline string getNumBufferMissMetricKey() const { return "numBufferMiss-" + to_string(id); }
    inline string getNumIOMetricKey() const { return "numIO-" + to_string(id); }

    void registerProfilingMetrics();

    void printTimeAndNumOutputMetrics(nlohmann::json& json, Profiler& profiler);
    void printBufferManagerMetrics(nlohmann::json& json, Profiler& profiler);

protected:
    ExecutionContext& context;
    uint32_t id;
    unique_ptr<OperatorMetrics> metrics;

    vector<unique_ptr<PhysicalOperator>> children;
    shared_ptr<ResultSet> resultSet;
};

} // namespace processor
} // namespace graphflow
