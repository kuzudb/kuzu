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

    inline function::TableFunction getTableFunc() const { return tableFunc; }

    inline function::TableFuncBindData* getBindData() const { return bindData.get(); }

    void computeFlatSchema() override;

    void computeFactorizedSchema() override;

    inline binder::expression_vector getOutputExpressions() const { return outputExpressions; }

    inline binder::Expression* getRowIDExpression() const { return rowIDExpression.get(); }

    inline std::string getExpressionsForPrinting() const override { return "CALL TABLE FUNC"; }

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
