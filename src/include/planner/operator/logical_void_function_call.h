#pragma once

#include "function/table/bind_data.h"
#include "function/table_functions.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalVoidFunctionCall : public LogicalOperator {
public:
    explicit LogicalVoidFunctionCall(function::TableFunction func,
        std::unique_ptr<function::TableFuncBindData> bindData)
        : LogicalOperator{LogicalOperatorType::VOID_FUNCTION_CALL}, func(std::move(func)),
          bindData(std::move(bindData)) {}

    inline function::TableFunction getFunc() const { return func; }
    const function::TableFuncBindData* getBindData() const { return bindData.get(); }

    inline std::string getExpressionsForPrinting() const override { return ""; }

    inline void computeFlatSchema() override { createEmptySchema(); }

    inline void computeFactorizedSchema() override { createEmptySchema(); }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalVoidFunctionCall>(func, bindData->copy());
    }

protected:
    function::TableFunction func;
    std::unique_ptr<function::TableFuncBindData> bindData;
};

} // namespace planner
} // namespace kuzu
