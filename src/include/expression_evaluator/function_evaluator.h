#pragma once

#include "expression_evaluator.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace evaluator {

class FunctionExpressionEvaluator : public ExpressionEvaluator {
    static constexpr EvaluatorType type_ = EvaluatorType::FUNCTION;

public:
    FunctionExpressionEvaluator(std::shared_ptr<binder::Expression> expression,
        std::vector<std::unique_ptr<ExpressionEvaluator>> children)
        : ExpressionEvaluator{type_, std::move(expression), std::move(children)}, execFunc{nullptr},
          selectFunc{nullptr}, bindData{nullptr} {}

    void init(const processor::ResultSet& resultSet, main::ClientContext* clientContext) override;

    void evaluate() override;

    bool select(common::SelectionVector& selVector) override;

    std::unique_ptr<ExpressionEvaluator> clone() override {
        return std::make_unique<FunctionExpressionEvaluator>(expression, cloneVector(children));
    }

protected:
    void resolveResultVector(const processor::ResultSet& resultSet,
        storage::MemoryManager* memoryManager) override;

private:
    function::scalar_func_exec_t execFunc;
    function::scalar_func_select_t selectFunc;
    std::vector<std::shared_ptr<common::ValueVector>> parameters;
    std::unique_ptr<function::FunctionBindData> bindData;
};

} // namespace evaluator
} // namespace kuzu
