#pragma once

#include "src/function/include/aggregate/aggregate_function.h"
#include "src/processor/include/physical_plan/operator/sink.h"

using namespace graphflow::function;

namespace graphflow {
namespace processor {

class BaseAggregateSharedState {

protected:
    BaseAggregateSharedState(const vector<unique_ptr<AggregateFunction>>& aggregateFunctions);

    unique_lock<mutex> acquireLock() { return unique_lock<mutex>{mtx}; }

    virtual bool hasMoreToRead() = 0;

    virtual pair<uint64_t, uint64_t> getNextRangeToRead() = 0;

protected:
    mutex mtx;
    uint64_t currentOffset;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;
};

class BaseAggregate : public Sink {

protected:
    BaseAggregate(vector<DataPos> aggregateVectorsPos,
        vector<unique_ptr<AggregateFunction>> aggregateFunctions,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : Sink{move(child), context, id}, aggregateVectorsPos{move(aggregateVectorsPos)},
          aggregateFunctions{move(aggregateFunctions)} {}

    PhysicalOperatorType getOperatorType() override { return AGGREGATE; }

    shared_ptr<ResultSet> initResultSet() override;

    void execute() override { Sink::execute(); }

    void finalize() override = 0;

    unique_ptr<PhysicalOperator> clone() override = 0;

protected:
    vector<DataPos> aggregateVectorsPos;
    vector<ValueVector*> aggregateVectors;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;
};

} // namespace processor
} // namespace graphflow
