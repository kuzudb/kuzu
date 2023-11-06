#pragma once

#include "processor/operator/order_by/sort_state.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/sink.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class OrderByMerge : public Sink {
public:
    // This constructor will only be called by the mapper when constructing the orderByMerge
    // operator, because the mapper doesn't know the existence of keyBlockMergeTaskDispatcher
    OrderByMerge(std::shared_ptr<SortSharedState> sharedState,
        std::shared_ptr<KeyBlockMergeTaskDispatcher> sharedDispatcher,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{nullptr /* resultSetDescriptor */, PhysicalOperatorType::ORDER_BY_MERGE,
              std::move(child), id, paramsString},
          sharedState{std::move(sharedState)}, sharedDispatcher{std::move(sharedDispatcher)} {}

    // This constructor is used for cloning only.
    OrderByMerge(std::shared_ptr<SortSharedState> sharedState,
        std::shared_ptr<KeyBlockMergeTaskDispatcher> sharedDispatcher, uint32_t id,
        const std::string& paramsString)
        : Sink{nullptr /* resultSetDescriptor */, PhysicalOperatorType::ORDER_BY_MERGE, id,
              paramsString},
          sharedState{std::move(sharedState)}, sharedDispatcher{std::move(sharedDispatcher)} {}

    inline bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<OrderByMerge>(sharedState, sharedDispatcher, id, paramsString);
    }

private:
    void initGlobalStateInternal(ExecutionContext* context) override;

private:
    std::shared_ptr<SortSharedState> sharedState;
    std::unique_ptr<KeyBlockMerger> localMerger;
    std::shared_ptr<KeyBlockMergeTaskDispatcher> sharedDispatcher;
};

} // namespace processor
} // namespace kuzu
