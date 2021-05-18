#pragma once

#include "src/common/include/vector/value_vector.h"
#include "src/expression_evaluator/include/expression_evaluator.h"

using namespace graphflow::common;

namespace graphflow {
namespace evaluator {

class BinaryExpressionEvaluator : public ExpressionEvaluator {

public:
    BinaryExpressionEvaluator(unique_ptr<ExpressionEvaluator> leftExpr,
        unique_ptr<ExpressionEvaluator> rightExpr, ExpressionType expressionType,
        DataType dataType);

    void evaluate() override;

    shared_ptr<ValueVector> createResultValueVector();

private:
    function<void(ValueVector&, ValueVector&, ValueVector&)> operation;
};

} // namespace evaluator
} // namespace graphflow
