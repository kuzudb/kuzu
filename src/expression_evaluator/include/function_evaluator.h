#pragma once

#include "base_evaluator.h"

#include "src/function/list/include/vector_list_operations.h"

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

    inline uint64_t select(sel_t* selectedPos) override {
        throw invalid_argument("Function " + expressionTypeToString(expression->expressionType) +
                               " does not support select interface");
    }

    unique_ptr<BaseExpressionEvaluator> clone() override;

private:
    void getFunction();

private:
    shared_ptr<Expression> expression;
    list_func func;
    vector<shared_ptr<ValueVector>> parameters;
};

} // namespace evaluator
} // namespace graphflow
