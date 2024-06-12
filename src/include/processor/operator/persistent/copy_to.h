#pragma once

#include "function/copy/copy_function.h"
#include "processor/operator/sink.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

struct CopyToInfo {
    function::CopyFunction copyFunc;
    std::unique_ptr<function::CopyFuncBindData> copyFuncBindData;
    std::vector<DataPos> inputVectorPoses;
    std::vector<bool> isFlatVec;

    CopyToInfo(function::CopyFunction copyFunc,
        std::unique_ptr<function::CopyFuncBindData> copyFuncBindData,
        std::vector<DataPos> inputVectorPoses, std::vector<bool> isFlatVec)
        : copyFunc{std::move(copyFunc)}, copyFuncBindData{std::move(copyFuncBindData)},
          inputVectorPoses{std::move(inputVectorPoses)}, isFlatVec{std::move(isFlatVec)} {}

    std::unique_ptr<CopyToInfo> copy() const {
        return std::make_unique<CopyToInfo>(copyFunc, copyFuncBindData->copy(), inputVectorPoses,
            isFlatVec);
    }
};

struct CopyToLocalState {
    std::unique_ptr<function::CopyFuncLocalState> copyFuncLocalState;
    std::vector<std::shared_ptr<common::ValueVector>> inputVectors;
};

class CopyTo final : public Sink {
public:
    CopyTo(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<CopyToInfo> info,
        std::shared_ptr<function::CopyFuncSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_TO, std::move(child), id,
              paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    void executeInternal(processor::ExecutionContext* context) override;

    bool isParallel() const override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyTo>(resultSetDescriptor->copy(), info->copy(), sharedState,
            children[0]->clone(), id, paramsString);
    }

private:
    std::unique_ptr<CopyToInfo> info;
    CopyToLocalState localState;
    std::shared_ptr<function::CopyFuncSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
