#pragma once

#include "processor/operator/sink.h"
#include "processor/result/result_set.h"
#include "sort_state.h"

namespace kuzu {
namespace processor {

class OrderBy : public Sink {
public:
    OrderBy(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<OrderByDataInfo> info, std::shared_ptr<SortSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::ORDER_BY, std::move(child), id,
              paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* /*context*/) override {
        // TODO(Ziyi): we always call lookup function on the first factorizedTable in sharedState
        // and that lookup function may read tuples in other factorizedTable, So we need to combine
        // hasNoNullGuarantee with other factorizedTables. This is not a good way to solve this
        // problem, and should be changed later.
        sharedState->combineFTHasNoNullGuarantee();
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<OrderBy>(resultSetDescriptor->copy(), info->copy(), sharedState,
            children[0]->clone(), id, paramsString);
    }

private:
    void initGlobalStateInternal(ExecutionContext* context) final;

private:
    std::unique_ptr<OrderByDataInfo> info;
    std::unique_ptr<SortLocalState> localState;
    std::shared_ptr<SortSharedState> sharedState;
    std::vector<common::ValueVector*> orderByVectors;
    std::vector<common::ValueVector*> payloadVectors;
};

} // namespace processor
} // namespace kuzu
