#pragma once

#include "src/processor/include/execution_context.h"
#include "src/processor/include/physical_plan/data_pos.h"
#include "src/processor/include/physical_plan/result/result_set.h"
#include "src/storage/include/buffer_manager.h"

using namespace std;

namespace graphflow {
namespace processor {

// Physical operator type
enum PhysicalOperatorType : uint8_t {
    SCAN,
    SELECT_SCAN,
    FILTER,
    INTERSECT,
    FLATTEN,
    READ_LIST,
    SCAN_ATTRIBUTE,
    PROJECTION,
    FRONTIER_EXTEND,
    HASH_JOIN_BUILD,
    HASH_JOIN_PROBE,
    RESULT_COLLECTOR,
    LOAD_CSV,
    MULTIPLICITY_REDUCER,
    LIMIT,
    SKIP,
    AGGREGATE,
    AGGREGATION_SCAN,
};

const string PhysicalOperatorTypeNames[] = {"SCAN", "SELECT_SCAN", "FILTER", "INTERSECT", "FLATTEN",
    "READ_LIST", "SCAN_ATTRIBUTE", "PROJECTION", "FRONTIER_EXTEND", "HASH_JOIN_BUILD",
    "HASH_JOIN_PROBE", "RESULT_COLLECTOR", "LOAD_CSV", "CREATE_NODE", "UPDATE_NODE", "DELETE_NODE",
    "MULTIPLICITY_REDUCER", "LIMIT", "SKIP"};

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
    PhysicalOperator(PhysicalOperatorType operatorType, ExecutionContext& context, uint32_t id)
        : PhysicalOperator(nullptr, operatorType, context, id) {}

    PhysicalOperator(unique_ptr<PhysicalOperator> prevOperator, PhysicalOperatorType operatorType,
        ExecutionContext& context, uint32_t id);

    virtual ~PhysicalOperator() = default;

    virtual void initResultSet(const shared_ptr<ResultSet>& resultSet);

    // For subquery, we rerun a plan multiple times. ReInitialize() should be called before each run
    // to ensure
    virtual void reInitialize();

    // Return false if no more tuples to pull, otherwise return true
    virtual bool getNextTuples() = 0;

    shared_ptr<ResultSet> getResultSet() { return resultSet; };

    virtual unique_ptr<PhysicalOperator> clone() = 0;

    virtual void printMetricsToJson(nlohmann::json& json, Profiler& profiler);

protected:
    string getTimeMetricKey() const { return "time-" + to_string(id); }
    string getNumTupleMetricKey() const { return "numTuple-" + to_string(id); }
    string getNumBufferHitMetricKey() const { return "numBufferHit-" + to_string(id); }
    string getNumBufferMissMetricKey() const { return "numBufferMiss-" + to_string(id); }
    string getNumIOMetricKey() const { return "numIO-" + to_string(id); }

    void registerProfilingMetrics();

    void printTimeAndNumOutputMetrics(nlohmann::json& json, Profiler& profiler);
    void printBufferManagerMetrics(nlohmann::json& json, Profiler& profiler);

public:
    unique_ptr<PhysicalOperator> prevOperator;
    PhysicalOperatorType operatorType;
    ExecutionContext& context;
    uint32_t id;
    unique_ptr<OperatorMetrics> metrics;

protected:
    shared_ptr<ResultSet> resultSet;
};

} // namespace processor
} // namespace graphflow
