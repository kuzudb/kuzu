#pragma once

#include "processor/operator/aggregate/base_aggregate_scan.h"
#include "processor/operator/aggregate/simple_aggregate.h"

namespace kuzu {
namespace processor {

class SimpleAggregateScan : public BaseAggregateScan {

public:
    SimpleAggregateScan(shared_ptr<SimpleAggregateSharedState> sharedState,
        vector<DataPos> aggregatesPos, vector<DataType> aggregateDataTypes,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseAggregateScan{move(aggregatesPos), move(aggregateDataTypes), move(child), id,
              paramsString},
          sharedState{move(sharedState)} {}

    // This constructor is used for cloning only.
    SimpleAggregateScan(shared_ptr<SimpleAggregateSharedState> sharedState,
        vector<DataPos> aggregatesPos, vector<DataType> aggregateDataTypes, uint32_t id,
        const string& paramsString)
        : BaseAggregateScan{move(aggregatesPos), move(aggregateDataTypes), id, paramsString},
          sharedState{move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    // SimpleAggregateScan is the source operator of a pipeline, so it should not clone its child.
    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SimpleAggregateScan>(
            sharedState, aggregatesPos, aggregateDataTypes, id, paramsString);
    }

private:
    shared_ptr<SimpleAggregateSharedState> sharedState;
    DataChunk* outDataChunk;
};

} // namespace processor
} // namespace kuzu
