#pragma once

#include "function/table/bind_data.h"
#include "function/table_functions.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalInQueryCall : public LogicalOperator {
public:
    LogicalInQueryCall(function::TableFunction tableFunc,
        std::unique_ptr<function::TableFuncBindData> bindData,
        binder::expression_vector outputExpressions,
        std::shared_ptr<binder::Expression> rowIDExpression)
        : LogicalOperator{LogicalOperatorType::IN_QUERY_CALL}, tableFunc{tableFunc},
          bindData{std::move(bindData)}, outputExpressions{std::move(outputExpressions)},
          rowIDExpression{std::move(rowIDExpression)} {}

    function::TableFunction getTableFunc() const { return tableFunc; }
    function::TableFuncBindData* getBindData() const { return bindData.get(); }

    void computeFlatSchema() override;
    void computeFactorizedSchema() override;

    binder::expression_vector getOutputExpressions() const { return outputExpressions; }

    binder::Expression* getRowIDExpression() const { return rowIDExpression.get(); }

    std::string getExpressionsForPrinting() const override { return tableFunc.name; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalInQueryCall>(tableFunc, bindData->copy(), outputExpressions,
            rowIDExpression);
    }

private:
    function::TableFunction tableFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    binder::expression_vector outputExpressions;
    std::shared_ptr<binder::Expression> rowIDExpression;
};

} // namespace planner
} // namespace kuzu
