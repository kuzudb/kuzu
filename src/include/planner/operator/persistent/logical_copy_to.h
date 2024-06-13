#pragma once

#include "function/export/export_function.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCopyTo : public LogicalOperator {
public:
    LogicalCopyTo(std::unique_ptr<function::ExportFuncBindData> bindData,
        function::ExportFunction exportFunc, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::COPY_TO, std::move(child)},
          bindData{std::move(bindData)}, exportFunc{std::move(exportFunc)} {}

    f_group_pos_set getGroupsPosToFlatten();

    std::string getExpressionsForPrinting() const override { return std::string{}; }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::unique_ptr<function::ExportFuncBindData> getBindData() const { return bindData->copy(); }
    function::ExportFunction getExportFunc() const { return exportFunc; };

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyTo>(bindData->copy(), exportFunc, children[0]->copy());
    }

private:
    std::unique_ptr<function::ExportFuncBindData> bindData;
    function::ExportFunction exportFunc;
};

} // namespace planner
} // namespace kuzu
