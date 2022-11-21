#pragma once

#include "base_evaluator.h"
#include "function/vector_operations.h"

using namespace kuzu::function;

namespace kuzu {
namespace evaluator {

class FunctionExpressionEvaluator : public BaseExpressionEvaluator {

public:
    FunctionExpressionEvaluator(
        shared_ptr<Expression> expression, vector<unique_ptr<BaseExpressionEvaluator>> children)
        : BaseExpressionEvaluator{move(children)}, expression{move(expression)} {}

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    void evaluate() override;

    bool select(SelectionVector& selVector) override;

    unique_ptr<BaseExpressionEvaluator> clone() override;

private:
    shared_ptr<Expression> expression;
    scalar_exec_func execFunc;
    scalar_select_func selectFunc;
    vector<shared_ptr<ValueVector>> parameters;
};

} // namespace evaluator
} // namespace kuzu
