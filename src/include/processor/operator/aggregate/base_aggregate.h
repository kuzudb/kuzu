#pragma once

#include "function/aggregate/aggregate_function.h"
#include "processor/operator/sink.h"

using namespace kuzu::function;

namespace kuzu {
namespace processor {

class BaseAggregateSharedState {

protected:
    explicit BaseAggregateSharedState(
        const vector<unique_ptr<AggregateFunction>>& aggregateFunctions);

    unique_lock<mutex> acquireLock() { return unique_lock<mutex>{mtx}; }

    virtual pair<uint64_t, uint64_t> getNextRangeToRead() = 0;

    virtual ~BaseAggregateSharedState() {}

protected:
    mutex mtx;
    uint64_t currentOffset;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;
};

class BaseAggregate : public Sink {

public:
    bool containDistinctAggregate() const;

protected:
    BaseAggregate(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> aggregateVectorsPos,
        vector<unique_ptr<AggregateFunction>> aggregateFunctions,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : Sink{std::move(resultSetDescriptor), std::move(child), id, paramsString},
          aggregateVectorsPos{std::move(aggregateVectorsPos)}, aggregateFunctions{
                                                                   std::move(aggregateFunctions)} {}

    PhysicalOperatorType getOperatorType() override { return AGGREGATE; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override = 0;

    unique_ptr<PhysicalOperator> clone() override = 0;

protected:
    vector<DataPos> aggregateVectorsPos;
    vector<ValueVector*> aggregateVectors;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;
};

} // namespace processor
} // namespace kuzu
