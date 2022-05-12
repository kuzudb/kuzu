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
        vector<DataType> aggregateDataTypes, unique_ptr<PhysicalOperator> child, uint32_t id)
        : BaseAggregateScan{move(resultSetDescriptor), move(aggregatesPos),
              move(aggregateDataTypes), move(child), id},
          groupByKeyVectorsPos{move(groupByKeyVectorsPos)},
          groupByKeyVectorDataTypes{move(groupByKeyVectorDataTypes)}, sharedState{
                                                                          move(sharedState)} {}

    HashAggregateScan(shared_ptr<HashAggregateSharedState> sharedState,
        unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> groupByKeyVectorsPos,
        vector<DataType> groupByKeyVectorDataTypes, vector<DataPos> aggregatesPos,
        vector<DataType> aggregateDataTypes, uint32_t id)
        : BaseAggregateScan{move(resultSetDescriptor), move(aggregatesPos),
              move(aggregateDataTypes), id},
          groupByKeyVectorsPos{move(groupByKeyVectorsPos)},
          groupByKeyVectorDataTypes{move(groupByKeyVectorDataTypes)}, sharedState{
                                                                          move(sharedState)} {}

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<HashAggregateScan>(sharedState, resultSetDescriptor->copy(),
            groupByKeyVectorsPos, groupByKeyVectorDataTypes, aggregatesPos, aggregateDataTypes, id);
    }

private:
    vector<DataPos> groupByKeyVectorsPos;
    vector<DataType> groupByKeyVectorDataTypes;
    vector<shared_ptr<ValueVector>> groupByKeyVectors;
    shared_ptr<HashAggregateSharedState> sharedState;
    vector<uint64_t> groupByKeyVectorsColIdxes;
};

} // namespace processor
} // namespace graphflow
