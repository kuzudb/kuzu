#pragma once

#include "processor/operator/aggregate/base_aggregate_scan.h"
#include "processor/operator/aggregate/hash_aggregate.h"

namespace kuzu {
namespace processor {

class HashAggregateScan : public BaseAggregateScan {
public:
    HashAggregateScan(shared_ptr<HashAggregateSharedState> sharedState,
        vector<DataPos> groupByKeyVectorsPos, vector<DataType> groupByKeyVectorDataTypes,
        vector<DataPos> aggregatesPos, vector<DataType> aggregateDataTypes,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseAggregateScan{std::move(aggregatesPos), std::move(aggregateDataTypes),
              std::move(child), id, paramsString},
          groupByKeyVectorsPos{std::move(groupByKeyVectorsPos)},
          groupByKeyVectorDataTypes{std::move(groupByKeyVectorDataTypes)}, sharedState{std::move(
                                                                               sharedState)} {}

    HashAggregateScan(shared_ptr<HashAggregateSharedState> sharedState,
        vector<DataPos> groupByKeyVectorsPos, vector<DataType> groupByKeyVectorDataTypes,
        vector<DataPos> aggregatesPos, vector<DataType> aggregateDataTypes, uint32_t id,
        const string& paramsString)
        : BaseAggregateScan{std::move(aggregatesPos), std::move(aggregateDataTypes), id,
              paramsString},
          groupByKeyVectorsPos{std::move(groupByKeyVectorsPos)},
          groupByKeyVectorDataTypes{std::move(groupByKeyVectorDataTypes)}, sharedState{std::move(
                                                                               sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashAggregateScan>(sharedState, groupByKeyVectorsPos,
            groupByKeyVectorDataTypes, aggregatesPos, aggregateDataTypes, id, paramsString);
    }

private:
    vector<DataPos> groupByKeyVectorsPos;
    vector<DataType> groupByKeyVectorDataTypes;
    vector<shared_ptr<ValueVector>> groupByKeyVectors;
    shared_ptr<HashAggregateSharedState> sharedState;
    vector<uint32_t> groupByKeyVectorsColIdxes;
};

} // namespace processor
} // namespace kuzu
