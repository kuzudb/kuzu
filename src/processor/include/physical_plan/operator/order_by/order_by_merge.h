#pragma once

#include "src/common/include/types.h"
#include "src/processor/include/physical_plan/operator/order_by/order_by.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

class OrderByMerge : public Sink, public SourceOperator {
public:
    // This constructor will only be called by the mapper when constructing the orderByMerge
    // operator, because the mapper doesn't know the existence of keyBlockMergeTaskDispatcher
    OrderByMerge(shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : Sink{move(child), context, id}, SourceOperator{nullptr},
          sharedFactorizedTablesAndSortedKeyBlocks{sharedState},
          keyBlockMergeTaskDispatcher{make_shared<KeyBlockMergeTaskDispatcher>()} {}

    // This constructor is used for cloning only.
    OrderByMerge(shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        shared_ptr<KeyBlockMergeTaskDispatcher> keyBlockMergeTaskDispatcher,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : Sink{move(child), context, id}, SourceOperator{nullptr},
          sharedFactorizedTablesAndSortedKeyBlocks{sharedState}, keyBlockMergeTaskDispatcher{
                                                                     keyBlockMergeTaskDispatcher} {}

    PhysicalOperatorType getOperatorType() override { return ORDER_BY_MERGE; }

    shared_ptr<ResultSet> initResultSet() override;
    void execute() override;
    void finalize() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<OrderByMerge>(sharedFactorizedTablesAndSortedKeyBlocks,
            keyBlockMergeTaskDispatcher, children[0]->clone(), context, id);
    }

private:
    shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedFactorizedTablesAndSortedKeyBlocks;
    unique_ptr<KeyBlockMerger> keyBlockMerger;
    shared_ptr<KeyBlockMergeTaskDispatcher> keyBlockMergeTaskDispatcher;
};

} // namespace processor
} // namespace graphflow
