#pragma once

#include "src/expression_evaluator/include/expression_evaluator.h"
#include "src/function/include/aggregation/aggregation_function.h"

using namespace graphflow::function;

namespace graphflow {
namespace evaluator {

class AggregateExpressionEvaluator : public ExpressionEvaluator {

public:
    static unique_ptr<AggregationFunction> getAggregationFunction(
        ExpressionType expressionType, DataType dataType);

public:
    AggregateExpressionEvaluator(ExpressionType expressionType, DataType dataType,
        unique_ptr<AggregationFunction> aggrFunction)
        : ExpressionEvaluator(expressionType, dataType), aggrFunction{move(aggrFunction)} {}

    AggregateExpressionEvaluator(ExpressionType expressionType, DataType dataType,
        unique_ptr<ExpressionEvaluator> child, unique_ptr<AggregationFunction> aggrFunction)
        : ExpressionEvaluator(expressionType, dataType), aggrFunction{move(aggrFunction)} {
        childrenExpr.push_back(move(child));
    }

    void initResultSet(const ResultSet& resultSet, MemoryManager& memoryManager) override;

    unique_ptr<ExpressionEvaluator> clone() override {
        if (childrenExpr.empty()) {
            return make_unique<AggregateExpressionEvaluator>(
                expressionType, dataType, aggrFunction->clone());
        } else {
            assert(childrenExpr.size() == 1);
            auto clonedChild = childrenExpr[0]->clone();
            return make_unique<AggregateExpressionEvaluator>(
                expressionType, dataType, move(clonedChild), aggrFunction->clone());
        }
    }

    void evaluate() override {
        if (childrenExpr.empty()) {
            return;
        }
        childrenExpr[0]->evaluate();
    }

    uint64_t select(sel_t* selectedPositions) override {
        throw invalid_argument("AggregationExpressionEvaluator doesn't support select.");
    }

    inline AggregationFunction* getFunction() { return aggrFunction.get(); }

    inline ValueVector* getChildResult() {
        return childResult == nullptr ? nullptr : childResult.get();
    }

private:
    static unique_ptr<AggregationFunction> getCountStarFunction();
    static unique_ptr<AggregationFunction> getCountFunction();
    static unique_ptr<AggregationFunction> getAvgFunction(DataType dataType);
    static unique_ptr<AggregationFunction> getSumFunction(DataType dataType);
    template<bool IS_MIN>
    static unique_ptr<AggregationFunction> getMinMaxFunction(DataType dataType);

    unique_ptr<AggregationFunction> aggrFunction;
    shared_ptr<ValueVector> childResult;
};
} // namespace evaluator
} // namespace graphflow
