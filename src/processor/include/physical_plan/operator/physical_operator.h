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
    DELETE_NODE
};

const string PhysicalOperatorTypeNames[] = {"SCAN", "FILTER", "FLATTEN", "READ_LIST",
    "SCAN_ATTRIBUTE", "PROJECTION", "FRONTIER_EXTEND", "HASH_JOIN_BUILD", "HASH_JOIN_PROBE",
    "RESULT_COLLECTOR", "LOAD_CSV", "CREATE_NODE", "UPDATE_NODE", "DELETE_NODE"};

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

    virtual void getNextTuples() = 0;

    shared_ptr<ResultSet> getResultSet() { return resultSet; };

    virtual unique_ptr<PhysicalOperator> clone() = 0;

    virtual nlohmann::json toJson(Profiler& profiler);

public:
    unique_ptr<PhysicalOperator> prevOperator;
    PhysicalOperatorType operatorType;
    bool isOutDataChunkFiltered;
    ExecutionContext& context;
    uint32_t id;
    TimeMetric* executionTime;
    NumericMetric* numOutputTuple;

protected:
    shared_ptr<ResultSet> resultSet;
};

} // namespace processor
} // namespace graphflow
