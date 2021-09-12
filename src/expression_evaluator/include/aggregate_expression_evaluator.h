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

    unique_ptr<ExpressionEvaluator> clone(
        MemoryManager& memoryManager, const ResultSet& resultSet) override {
        return nullptr;
    }

    uint64_t select(sel_t* selectedPositions) override { return 0; }

    inline AggregationFunction* getFunction() { return aggrFunction.get(); }

private:
    static unique_ptr<AggregationFunction> getCountStarFunction();
    static unique_ptr<AggregationFunction> getCountFunction();
    static unique_ptr<AggregationFunction> getAvgFunction(DataType dataType);
    static unique_ptr<AggregationFunction> getSumFunction(DataType dataType);
    template<bool IS_MIN>
    static unique_ptr<AggregationFunction> getMinMaxFunction(DataType dataType);

    unique_ptr<AggregationFunction> aggrFunction;
};
} // namespace evaluator
} // namespace graphflow
