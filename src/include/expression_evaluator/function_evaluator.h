#pragma once

#include "binder/expression/function_expression.h"
#include "expression_evaluator.h"
#include "function/lambda/lambda_function_bind_data.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace evaluator {

class FunctionExpressionEvaluator : public ExpressionEvaluator {
public:
    FunctionExpressionEvaluator(std::shared_ptr<binder::Expression> expression,
        std::vector<std::unique_ptr<ExpressionEvaluator>> children)
        : ExpressionEvaluator{std::move(children)}, expression{std::move(expression)},
          execFunc{nullptr}, selectFunc{nullptr}, bindData{nullptr} {}

    void init(const processor::ResultSet& resultSet, main::ClientContext* clientContext) override;

    void evaluate() override;

    bool select(common::SelectionVector& selVector) override;

    std::unique_ptr<ExpressionEvaluator> clone() override;

protected:
    void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) override;

private:
    std::shared_ptr<binder::Expression> expression;
    function::scalar_func_exec_t execFunc;
    function::scalar_func_select_t selectFunc;
    std::vector<std::shared_ptr<common::ValueVector>> parameters;
    std::unique_ptr<function::FunctionBindData> bindData;
};

} // namespace evaluator
} // namespace kuzu
