#pragma once

#include "src/processor/include/execution_context.h"
#include "src/processor/include/physical_plan/operator/result/result_set.h"

using namespace graphflow::transaction;
using namespace std;

namespace graphflow {
namespace processor {

// Physical operator type
enum PhysicalOperatorType : uint8_t {
    SCAN,
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
    CREATE_NODE,
    UPDATE_NODE,
    DELETE_NODE,
    MULTIPLICITY_REDUCER,
    LIMIT,
    SKIP
};

const string PhysicalOperatorTypeNames[] = {"SCAN", "FILTER", "INTERSECT", "FLATTEN", "READ_LIST",
    "SCAN_ATTRIBUTE", "PROJECTION", "FRONTIER_EXTEND", "HASH_JOIN_BUILD", "HASH_JOIN_PROBE",
    "RESULT_COLLECTOR", "LOAD_CSV", "CREATE_NODE", "UPDATE_NODE", "DELETE_NODE",
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
        : PhysicalOperator(nullptr, operatorType, false, context, id) {}

    PhysicalOperator(unique_ptr<PhysicalOperator> prevOperator, PhysicalOperatorType operatorType,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator(move(prevOperator), operatorType, false, context, id) {}

    PhysicalOperator(unique_ptr<PhysicalOperator> prevOperator, PhysicalOperatorType operatorType,
        bool isOutDataChunkFiltered, ExecutionContext& context, uint32_t id);

    virtual ~PhysicalOperator() = default;

    // For subquery, we rerun a plan multiple times. ReInitialize() should be called before each run
    // to ensure
    virtual void reInitialize() { prevOperator->reInitialize(); }

    virtual void getNextTuples() = 0;

    shared_ptr<ResultSet> getResultSet() { return resultSet; };

    virtual unique_ptr<PhysicalOperator> clone() = 0;

    virtual void printMetricsToJson(nlohmann::json& json, Profiler& profiler);

protected:
    string getTimeMetricKey() { return "time-" + to_string(id); }
    string getNumTupleMetricKey() { return "numTuple-" + to_string(id); }
    string getNumBufferHitMetricKey() { return "numBufferHit-" + to_string(id); }
    string getNumBufferMissMetricKey() { return "numBufferMiss-" + to_string(id); }
    string getNumIOMetricKey() { return "numIO-" + to_string(id); }

    void registerProfilingMetrics();

    void printTimeAndNumOutputMetrics(nlohmann::json& json, Profiler& profiler);
    void printBufferManagerMetrics(nlohmann::json& json, Profiler& profiler);

public:
    unique_ptr<PhysicalOperator> prevOperator;
    PhysicalOperatorType operatorType;
    bool isOutDataChunkFiltered;
    ExecutionContext& context;
    uint32_t id;
    unique_ptr<OperatorMetrics> metrics;

protected:
    shared_ptr<ResultSet> resultSet;
};

} // namespace processor
} // namespace graphflow
