#pragma once

#include "processor/operator/aggregate/base_aggregate_scan.h"
#include "processor/operator/aggregate/simple_aggregate.h"

namespace kuzu {
namespace processor {

class SimpleAggregateScan : public BaseAggregateScan {
public:
    SimpleAggregateScan(std::shared_ptr<SimpleAggregateSharedState> sharedState,
        std::vector<DataPos> aggregatesPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : BaseAggregateScan{std::move(aggregatesPos), std::move(child), id, paramsString},
          sharedState{std::move(sharedState)}, outDataChunk{nullptr} {}

    // This constructor is used for cloning only.
    SimpleAggregateScan(std::shared_ptr<SimpleAggregateSharedState> sharedState,
        std::vector<DataPos> aggregatesPos, uint32_t id, const std::string& paramsString)
        : BaseAggregateScan{std::move(aggregatesPos), id, paramsString},
          sharedState{std::move(sharedState)}, outDataChunk{nullptr} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    // SimpleAggregateScan is the source operator of a pipeline, so it should not clone its child.
    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SimpleAggregateScan>(sharedState, aggregatesPos, id, paramsString);
    }

private:
    std::shared_ptr<SimpleAggregateSharedState> sharedState;
    common::DataChunk* outDataChunk;
};

} // namespace processor
} // namespace kuzu
