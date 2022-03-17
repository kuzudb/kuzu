#pragma once

#include "base_evaluator.h"

#include "src/function/include/vector_operations.h"

using namespace graphflow::function;

namespace graphflow {
namespace evaluator {

class FunctionExpressionEvaluator : public BaseExpressionEvaluator {

public:
    FunctionExpressionEvaluator(
        shared_ptr<Expression> expression, vector<unique_ptr<BaseExpressionEvaluator>> children)
        : BaseExpressionEvaluator{move(children)}, expression{move(expression)} {}

    void init(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    void evaluate() override;

    uint64_t select(sel_t* selectedPos) override;

    unique_ptr<BaseExpressionEvaluator> clone() override;

private:
    void getExecFunction();

    void getSelectFunction();

private:
    shared_ptr<Expression> expression;
    scalar_exec_func execFunc;
    scalar_select_func selectFunc;
    vector<shared_ptr<ValueVector>> parameters;
};

} // namespace evaluator
} // namespace graphflow
