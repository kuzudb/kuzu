#pragma once

#include "catalog/catalog.h"
#include "function/table_functions.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct InQueryCallSharedState {
    common::offset_t offset = 0;
    common::offset_t maxOffset;
    std::mutex mtx;

    explicit InQueryCallSharedState(common::offset_t maxOffset) : maxOffset{maxOffset} {}

    std::pair<common::offset_t, common::offset_t> getNextBatch();
};

struct InQueryCallInfo {
    function::table_func_t tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    std::vector<DataPos> outputPoses;

    InQueryCallInfo(function::table_func_t tableFunc,
        std::unique_ptr<function::TableFuncBindData> bindData, std::vector<DataPos> outputPoses)
        : tableFunc{std::move(tableFunc)}, bindData{std::move(bindData)}, outputPoses{std::move(
                                                                              outputPoses)} {}

    std::unique_ptr<InQueryCallInfo> copy() {
        return std::make_unique<InQueryCallInfo>(tableFunc, bindData->copy(), outputPoses);
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
