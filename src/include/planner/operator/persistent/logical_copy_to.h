#pragma once

#include "common/copier_config/csv_reader_config.h"
#include "common/copier_config/reader_config.h"
#include "function/copy/copy_function.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCopyTo : public LogicalOperator {
public:
    LogicalCopyTo(std::unique_ptr<function::CopyFuncBindData> bindData,
        function::CopyFunction copyFunc, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::COPY_TO, std::move(child)},
          bindData{std::move(bindData)}, copyFunc{std::move(copyFunc)} {}

    f_group_pos_set getGroupsPosToFlatten();

    std::string getExpressionsForPrinting() const override { return std::string{}; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::unique_ptr<function::CopyFuncBindData> getBindData() const { return bindData->copy(); }
    function::CopyFunction getCopyFunc() const { return copyFunc; };

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyTo>(bindData->copy(), copyFunc, children[0]->copy());
    }

private:
    std::unique_ptr<function::CopyFuncBindData> bindData;
    function::CopyFunction copyFunc;
};

} // namespace planner
} // namespace kuzu
