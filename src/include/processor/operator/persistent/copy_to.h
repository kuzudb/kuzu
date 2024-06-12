#pragma once

#include "function/copy/copy_function.h"
#include "processor/operator/sink.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

class CopyTo final : public Sink {
public:
    CopyTo(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        function::CopyFunction copyFunc, std::unique_ptr<function::CopyFuncBindData> bindData,
        std::shared_ptr<function::CopyFuncSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_TO, std::move(child), id,
              paramsString},
          copyFunc{std::move(copyFunc)}, bindData{std::move(bindData)},
          sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override;

    void executeInternal(processor::ExecutionContext* context) override;

    bool isParallel() const override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CopyTo>(resultSetDescriptor->copy(), copyFunc, bindData->copy(),
            sharedState, children[0]->clone(), id, paramsString);
    }

private:
    function::CopyFunction copyFunc;
    std::unique_ptr<function::CopyFuncBindData> bindData;
    std::unique_ptr<function::CopyFuncLocalState> localState;
    std::shared_ptr<function::CopyFuncSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
