#pragma once

#include "processor/operator/order_by/order_by.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"
#include "processor/operator/source_operator.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

class OrderByMerge : public Sink, public SourceOperator {
public:
    // This constructor will only be called by the mapper when constructing the orderByMerge
    // operator, because the mapper doesn't know the existence of keyBlockMergeTaskDispatcher
    OrderByMerge(shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        shared_ptr<KeyBlockMergeTaskDispatcher> sharedDispatcher,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : Sink{std::move(child), id, paramsString}, SourceOperator{nullptr},
          sharedState{std::move(sharedState)}, sharedDispatcher{std::move(sharedDispatcher)} {}

    // This constructor is used for cloning only.
    OrderByMerge(shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        shared_ptr<KeyBlockMergeTaskDispatcher> sharedDispatcher, uint32_t id,
        const string& paramsString)
        : Sink{id, paramsString}, SourceOperator{nullptr}, sharedState{std::move(sharedState)},
          sharedDispatcher{std::move(sharedDispatcher)} {}

    PhysicalOperatorType getOperatorType() override { return ORDER_BY_MERGE; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<OrderByMerge>(sharedState, sharedDispatcher, id, paramsString);
    }

    inline double getExecutionTime(Profiler& profiler) const override {
        return profiler.sumAllTimeMetricsWithKey(getTimeMetricKey());
    }

private:
    void initGlobalStateInternal(ExecutionContext* context) override;

private:
    shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState;
    unique_ptr<KeyBlockMerger> localMerger;
    shared_ptr<KeyBlockMergeTaskDispatcher> sharedDispatcher;
};

} // namespace processor
} // namespace kuzu
