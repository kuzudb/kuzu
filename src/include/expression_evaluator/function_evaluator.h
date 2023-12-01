#pragma once

#include "expression_evaluator.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace evaluator {

class FunctionExpressionEvaluator : public ExpressionEvaluator {
public:
    FunctionExpressionEvaluator(std::shared_ptr<binder::Expression> expression,
        std::vector<std::unique_ptr<ExpressionEvaluator>> children)
        : ExpressionEvaluator{std::move(children)},
          expression{std::move(expression)}, execFunc{nullptr}, selectFunc{nullptr} {}

    void init(
        const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) override;

    void evaluate() override;

    bool select(common::SelectionVector& selVector) override;

    std::unique_ptr<ExpressionEvaluator> clone() override;

protected:
    void resolveResultVector(
        const processor::ResultSet& resultSet, storage::MemoryManager* memoryManager) override;

private:
    std::shared_ptr<binder::Expression> expression;
    function::scalar_exec_func execFunc;
    function::scalar_select_func selectFunc;
    std::vector<std::shared_ptr<common::ValueVector>> parameters;
};

} // namespace evaluator
} // namespace kuzu
