#pragma once

#include "processor/operator/aggregate/base_aggregate_scan.h"
#include "processor/operator/aggregate/hash_aggregate.h"

namespace kuzu {
namespace processor {

class HashAggregateScan : public BaseAggregateScan {
public:
    HashAggregateScan(std::shared_ptr<HashAggregateSharedState> sharedState,
        std::vector<DataPos> groupByKeyVectorsPos, std::vector<DataPos> aggregatesPos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : BaseAggregateScan{std::move(aggregatesPos), std::move(child), id, paramsString},
          groupByKeyVectorsPos{std::move(groupByKeyVectorsPos)}, sharedState{
                                                                     std::move(sharedState)} {}

    HashAggregateScan(std::shared_ptr<HashAggregateSharedState> sharedState,
        std::vector<DataPos> groupByKeyVectorsPos, std::vector<DataPos> aggregatesPos, uint32_t id,
        const std::string& paramsString)
        : BaseAggregateScan{std::move(aggregatesPos), id, paramsString},
          groupByKeyVectorsPos{std::move(groupByKeyVectorsPos)}, sharedState{
                                                                     std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<HashAggregateScan>(
            sharedState, groupByKeyVectorsPos, aggregatesPos, id, paramsString);
    }

private:
    std::vector<DataPos> groupByKeyVectorsPos;
    std::vector<common::ValueVector*> groupByKeyVectors;
    std::shared_ptr<HashAggregateSharedState> sharedState;
    std::vector<uint32_t> groupByKeyVectorsColIdxes;
};

} // namespace processor
} // namespace kuzu
