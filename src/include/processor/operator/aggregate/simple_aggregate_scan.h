#pragma once

#include "processor/operator/aggregate/base_aggregate_scan.h"
#include "processor/operator/aggregate/simple_aggregate.h"

namespace kuzu {
namespace processor {

class SimpleAggregateScan final : public BaseAggregateScan {
public:
    SimpleAggregateScan(std::shared_ptr<SimpleAggregateSharedState> sharedState,
        std::vector<DataPos> aggregatesPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : BaseAggregateScan{std::move(aggregatesPos), std::move(child), id, std::move(printInfo)},
          sharedState{std::move(sharedState)}, outDataChunk{nullptr} {}

    SimpleAggregateScan(std::shared_ptr<SimpleAggregateSharedState> sharedState,
        std::vector<DataPos> aggregatesPos, uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : BaseAggregateScan{std::move(aggregatesPos), id, std::move(printInfo)},
          sharedState{std::move(sharedState)}, outDataChunk{nullptr} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    // SimpleAggregateScan is the source operator of a pipeline, so it should not clone its child.
    std::unique_ptr<PhysicalOperator> copy() override {
        return make_unique<SimpleAggregateScan>(sharedState, aggregatesPos, id, printInfo->copy());
    }

private:
    std::shared_ptr<SimpleAggregateSharedState> sharedState;
    common::DataChunk* outDataChunk;
};

} // namespace processor
} // namespace kuzu
