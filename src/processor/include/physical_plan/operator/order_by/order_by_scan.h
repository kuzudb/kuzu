#pragma once

#include "src/processor/include/physical_plan/operator/order_by/order_by.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"

namespace graphflow {
namespace processor {

// To preserve the ordering of tuples, the orderByScan operator will only
// be executed in single-thread mode.
class OrderByScan : public PhysicalOperator, public SourceOperator {

public:
    OrderByScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        const vector<DataPos>& outDataPoses,
        shared_ptr<SharedRowCollectionsAndSortedKeyBlocks> sharedState,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(child), context, id}, SourceOperator{move(resultSetDescriptor)},
          outDataPoses{outDataPoses}, sharedState{sharedState}, nextRowIdxToReadInMemBlock{0} {}

    // This constructor is used for cloning only.
    OrderByScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        const vector<DataPos>& outDataPoses,
        shared_ptr<SharedRowCollectionsAndSortedKeyBlocks> sharedState, ExecutionContext& context,
        uint32_t id)
        : PhysicalOperator{context, id}, SourceOperator{move(resultSetDescriptor)},
          outDataPoses{outDataPoses}, sharedState{sharedState}, nextRowIdxToReadInMemBlock{0} {}

    PhysicalOperatorType getOperatorType() override { return ORDER_BY_SCAN; }

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<OrderByScan>(
            resultSetDescriptor->copy(), outDataPoses, sharedState, context, id);
    }

private:
    vector<DataPos> outDataPoses;
    shared_ptr<SharedRowCollectionsAndSortedKeyBlocks> sharedState;
    vector<uint64_t> fieldsToReadInRowCollection;
    uint64_t nextRowIdxToReadInMemBlock;
};

} // namespace processor
} // namespace graphflow
