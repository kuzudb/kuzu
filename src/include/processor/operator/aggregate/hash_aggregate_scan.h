#pragma once

#include "processor/operator/aggregate/base_aggregate_scan.h"
#include "processor/operator/aggregate/hash_aggregate.h"

namespace kuzu {
namespace processor {

class HashAggregateScan : public BaseAggregateScan {

public:
    HashAggregateScan(shared_ptr<HashAggregateSharedState> sharedState,
        unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> groupByKeyVectorsPos,
        vector<DataType> groupByKeyVectorDataTypes, vector<DataPos> aggregatesPos,
        vector<DataType> aggregateDataTypes, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : BaseAggregateScan{move(resultSetDescriptor), move(aggregatesPos),
              move(aggregateDataTypes), move(child), id, paramsString},
          groupByKeyVectorsPos{move(groupByKeyVectorsPos)},
          groupByKeyVectorDataTypes{move(groupByKeyVectorDataTypes)}, sharedState{
                                                                          move(sharedState)} {}

    HashAggregateScan(shared_ptr<HashAggregateSharedState> sharedState,
        unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> groupByKeyVectorsPos,
        vector<DataType> groupByKeyVectorDataTypes, vector<DataPos> aggregatesPos,
        vector<DataType> aggregateDataTypes, uint32_t id, const string& paramsString)
        : BaseAggregateScan{move(resultSetDescriptor), move(aggregatesPos),
              move(aggregateDataTypes), id, paramsString},
          groupByKeyVectorsPos{move(groupByKeyVectorsPos)},
          groupByKeyVectorDataTypes{move(groupByKeyVectorDataTypes)}, sharedState{
                                                                          move(sharedState)} {}

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashAggregateScan>(sharedState, resultSetDescriptor->copy(),
            groupByKeyVectorsPos, groupByKeyVectorDataTypes, aggregatesPos, aggregateDataTypes, id,
            paramsString);
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
