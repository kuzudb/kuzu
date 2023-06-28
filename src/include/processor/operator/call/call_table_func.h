#pragma once

#include "catalog/catalog.h"
#include "function/table_operations.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct CallTableFuncSharedState {
    common::offset_t offset = 0;
    common::offset_t maxOffset;
    std::mutex mtx;

    explicit CallTableFuncSharedState(common::offset_t maxOffset) : maxOffset{maxOffset} {}

    inline bool hasNext() {
        std::lock_guard guard{mtx};
        return offset < maxOffset;
    }

    std::pair<common::offset_t, common::offset_t> getNextBatch();
};

struct CallTableFuncInfo {
    function::table_func_t tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::vector<DataPos> outputPoses;

    CallTableFuncInfo(function::table_func_t tableFunc,
        std::unique_ptr<function::TableFuncBindData> bindData, std::vector<DataPos> outputPoses)
        : tableFunc{std::move(tableFunc)}, bindData{std::move(bindData)}, outputPoses{std::move(
                                                                              outputPoses)} {}

    std::unique_ptr<CallTableFuncInfo> copy() {
        return std::make_unique<CallTableFuncInfo>(tableFunc, bindData->copy(), outputPoses);
    }
};

class CallTableFunc : public PhysicalOperator {
public:
    CallTableFunc(std::unique_ptr<CallTableFuncInfo> localState,
        std::shared_ptr<CallTableFuncSharedState> sharedState, PhysicalOperatorType operatorType,
        uint32_t id, const std::string& paramsString)
        : callTableFuncInfo{std::move(localState)}, sharedState{std::move(sharedState)},
          PhysicalOperator{operatorType, id, paramsString} {}

    inline bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CallTableFunc>(
            callTableFuncInfo->copy(), sharedState, operatorType, id, paramsString);
    }

protected:
    std::unique_ptr<CallTableFuncInfo> callTableFuncInfo;
    std::shared_ptr<CallTableFuncSharedState> sharedState;
    std::vector<common::ValueVector*> outputVectors;
};

} // namespace processor
} // namespace kuzu
