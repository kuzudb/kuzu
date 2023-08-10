#pragma once

#include "function/table_functions.h"
#include "logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalInQueryCall : public LogicalOperator {
public:
    LogicalInQueryCall(function::table_func_t tableFunc,
        std::unique_ptr<function::TableFuncBindData> bindData,
        binder::expression_vector outputExpressions)
        : LogicalOperator{LogicalOperatorType::IN_QUERY_CALL}, tableFunc{std::move(tableFunc)},
          bindData{std::move(bindData)}, outputExpressions{std::move(outputExpressions)} {}

    inline function::table_func_t getTableFunc() const { return tableFunc; }

    inline function::TableFuncBindData* getBindData() const { return bindData.get(); }

    void computeFlatSchema() override;

    void computeFactorizedSchema() override;

    inline binder::expression_vector getOutputExpressions() const { return outputExpressions; }

    inline std::string getExpressionsForPrinting() const override { return "CALL TABLE FUNC"; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalInQueryCall>(tableFunc, bindData->copy(), outputExpressions);
    }

private:
    function::table_func_t tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    binder::expression_vector outputExpressions;
};

} // namespace planner
} // namespace kuzu
