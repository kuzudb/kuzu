#pragma once

#include "function/export/export_function.h"
#include "processor/operator/sink.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

struct CopyToInfo {
    function::ExportFunction exportFunc;
    std::unique_ptr<function::ExportFuncBindData> bindData;
    std::vector<DataPos> inputVectorPoses;
    std::vector<bool> isFlatVec;

    CopyToInfo(function::ExportFunction exportFunc,
        std::unique_ptr<function::ExportFuncBindData> bindData,
        std::vector<DataPos> inputVectorPoses, std::vector<bool> isFlatVec)
        : exportFunc{std::move(exportFunc)}, bindData{std::move(bindData)},
          inputVectorPoses{std::move(inputVectorPoses)}, isFlatVec{std::move(isFlatVec)} {}

    CopyToInfo copy() const {
        return CopyToInfo{exportFunc, bindData->copy(), inputVectorPoses, isFlatVec};
    }
};

struct CopyToLocalState {
    std::unique_ptr<function::ExportFuncLocalState> exportFuncLocalState;
    std::vector<std::shared_ptr<common::ValueVector>> inputVectors;
};

class CopyTo final : public Sink {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::COPY_TO;

public:
    CopyTo(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor, CopyToInfo info,
        std::shared_ptr<function::ExportFuncSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : Sink{std::move(resultSetDescriptor), type_, std::move(child), id, std::move(printInfo)},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    void executeInternal(processor::ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyTo>(resultSetDescriptor->copy(), info.copy(), sharedState,
            children[0]->clone(), id, printInfo->copy());
    }

private:
    CopyToInfo info;
    CopyToLocalState localState;
    std::shared_ptr<function::ExportFuncSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
