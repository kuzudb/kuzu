#pragma once

#include "src/processor/include/physical_plan/operator/aggregate/base_aggregate_scan.h"
#include "src/processor/include/physical_plan/operator/aggregate/hash_aggregate.h"

namespace graphflow {
namespace processor {

class HashAggregateScan : public BaseAggregateScan {

public:
    HashAggregateScan(shared_ptr<HashAggregateSharedState> sharedState,
        unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> groupByKeyVectorsPos,
        vector<DataType> groupByKeyVectorDataTypes, vector<DataPos> aggregatesPos,
        vector<DataType> aggregateDataTypes, unique_ptr<PhysicalOperator> child,
        ExecutionContext& context, uint32_t id)
        : BaseAggregateScan{move(resultSetDescriptor), move(aggregatesPos),
              move(aggregateDataTypes), move(child), context, id},
          groupByKeyVectorsPos{move(groupByKeyVectorsPos)},
          groupByKeyVectorDataTypes{move(groupByKeyVectorDataTypes)}, sharedState{
                                                                          move(sharedState)} {}

    HashAggregateScan(shared_ptr<HashAggregateSharedState> sharedState,
        unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> groupByKeyVectorsPos,
        vector<DataType> groupByKeyVectorDataTypes, vector<DataPos> aggregatesPos,
        vector<DataType> aggregateDataTypes, ExecutionContext& context, uint32_t id)
        : BaseAggregateScan{move(resultSetDescriptor), move(aggregatesPos),
              move(aggregateDataTypes), context, id},
          groupByKeyVectorsPos{move(groupByKeyVectorsPos)},
          groupByKeyVectorDataTypes{move(groupByKeyVectorDataTypes)}, sharedState{
                                                                          move(sharedState)} {}

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashAggregateScan>(sharedState, resultSetDescriptor->copy(),
            groupByKeyVectorsPos, groupByKeyVectorDataTypes, aggregatesPos, aggregateDataTypes,
            context, id);
    }

private:
    vector<DataPos> groupByKeyVectorsPos;
    vector<DataType> groupByKeyVectorDataTypes;
    vector<ValueVector*> groupByKeyVectors;

    shared_ptr<HashAggregateSharedState> sharedState;
};

} // namespace processor
} // namespace graphflow
