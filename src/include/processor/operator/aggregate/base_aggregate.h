#pragma once

#include "aggregate_input.h"
#include "function/aggregate_function.h"
#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

class BaseAggregateSharedState {
protected:
    explicit BaseAggregateSharedState(
        const std::vector<function::AggregateFunction>& aggregateFunctions);

    virtual std::pair<uint64_t, uint64_t> getNextRangeToRead() = 0;

    ~BaseAggregateSharedState() = default;

protected:
    std::mutex mtx;
    uint64_t currentOffset;
    std::vector<function::AggregateFunction> aggregateFunctions;
};

class BaseAggregate : public Sink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::AGGREGATE;

protected:
    BaseAggregate(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::vector<function::AggregateFunction> aggregateFunctions,
        std::vector<AggregateInfo> aggInfos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{std::move(resultSetDescriptor), type_, std::move(child), id, std::move(printInfo)},
          aggregateFunctions{std::move(aggregateFunctions)}, aggInfos{std::move(aggInfos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool isParallel() const final { return !containDistinctAggregate(); }

    void finalizeInternal(ExecutionContext* context) override = 0;

    std::unique_ptr<PhysicalOperator> clone() override = 0;

private:
    bool containDistinctAggregate() const;

protected:
    std::vector<function::AggregateFunction> aggregateFunctions;
    std::vector<AggregateInfo> aggInfos;
    std::vector<AggregateInput> aggInputs;
};

} // namespace processor
} // namespace kuzu
