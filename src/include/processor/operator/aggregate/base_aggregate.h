#pragma once

#include "function/aggregate/aggregate_function.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

class BaseAggregateSharedState {

protected:
    explicit BaseAggregateSharedState(
        const std::vector<std::unique_ptr<function::AggregateFunction>>& aggregateFunctions);

    std::unique_lock<std::mutex> acquireLock() { return std::unique_lock<std::mutex>{mtx}; }

    virtual std::pair<uint64_t, uint64_t> getNextRangeToRead() = 0;

    virtual ~BaseAggregateSharedState() {}

protected:
    std::mutex mtx;
    uint64_t currentOffset;
    std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions;
};

class BaseAggregate : public Sink {

public:
    bool containDistinctAggregate() const;

protected:
    BaseAggregate(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::vector<DataPos> aggregateVectorsPos,
        std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::AGGREGATE, std::move(child),
              id, paramsString},
          aggregateVectorsPos{std::move(aggregateVectorsPos)}, aggregateFunctions{
                                                                   std::move(aggregateFunctions)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override = 0;

    std::unique_ptr<PhysicalOperator> clone() override = 0;

protected:
    std::vector<DataPos> aggregateVectorsPos;
    std::vector<common::ValueVector*> aggregateVectors;
    std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions;
};

} // namespace processor
} // namespace kuzu
