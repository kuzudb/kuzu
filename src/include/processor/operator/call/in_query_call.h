#pragma once

#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct InQueryCallSharedState {
    std::unique_ptr<function::SharedTableFuncState> sharedState;
};

struct InQueryCallInfo {
    function::TableFunction* function;
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::vector<DataPos> outputPoses;

    InQueryCallInfo(function::TableFunction* function,
        std::unique_ptr<function::TableFuncBindData> bindData, std::vector<DataPos> outputPoses)
        : function{function}, bindData{std::move(bindData)}, outputPoses{std::move(outputPoses)} {}

    std::unique_ptr<InQueryCallInfo> copy() {
        return std::make_unique<InQueryCallInfo>(function, bindData->copy(), outputPoses);
    }
};

class InQueryCall : public PhysicalOperator {
public:
    InQueryCall(std::unique_ptr<InQueryCallInfo> localState,
        std::shared_ptr<InQueryCallSharedState> sharedState, PhysicalOperatorType operatorType,
        uint32_t id, const std::string& paramsString)
        : inQueryCallInfo{std::move(localState)}, sharedState{std::move(sharedState)},
          PhysicalOperator{operatorType, id, paramsString} {}

    inline bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void initGlobalStateInternal(ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<InQueryCall>(
            inQueryCallInfo->copy(), sharedState, operatorType, id, paramsString);
    }

protected:
    std::unique_ptr<InQueryCallInfo> inQueryCallInfo;
    std::shared_ptr<InQueryCallSharedState> sharedState;
    std::vector<common::ValueVector*> outputVectors;
};

} // namespace processor
} // namespace kuzu
