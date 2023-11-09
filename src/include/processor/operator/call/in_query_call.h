#pragma once

#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct InQueryCallSharedState {
    std::unique_ptr<function::TableFuncSharedState> sharedState;
    common::row_idx_t nextRowIdx = 0;
    std::mutex mtx;

    common::row_idx_t getAndIncreaseRowIdx(uint64_t numRows);
};

struct InQueryCallLocalState {
    std::unique_ptr<common::DataChunk> outputChunk;
    common::ValueVector* rowIDVector;
    std::unique_ptr<function::TableFuncLocalState> localState;
    function::TableFunctionInput tableFunctionInput;
};

struct InQueryCallInfo {
    function::TableFunction* function;
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::vector<DataPos> outputPoses;
    DataPos rowIDPos;

    InQueryCallInfo(function::TableFunction* function,
        std::unique_ptr<function::TableFuncBindData> bindData, std::vector<DataPos> outputPoses,
        DataPos rowIDPos)
        : function{function}, bindData{std::move(bindData)},
          outputPoses{std::move(outputPoses)}, rowIDPos{std::move(rowIDPos)} {}

    std::unique_ptr<InQueryCallInfo> copy() {
        return std::make_unique<InQueryCallInfo>(function, bindData->copy(), outputPoses, rowIDPos);
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

    inline bool canParallel() const override {
        return inQueryCallInfo->function->canParallelFunc();
    }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void initGlobalStateInternal(ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<InQueryCall>(
            inQueryCallInfo->copy(), sharedState, operatorType, id, paramsString);
    }

    inline InQueryCallSharedState* getSharedState() { return sharedState.get(); }

protected:
    std::unique_ptr<InQueryCallInfo> inQueryCallInfo;
    std::shared_ptr<InQueryCallSharedState> sharedState;
    std::unique_ptr<InQueryCallLocalState> localState;
};

} // namespace processor
} // namespace kuzu
