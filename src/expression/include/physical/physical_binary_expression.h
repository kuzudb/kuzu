#pragma once

#include "include/physical/physical_expression.h"
#include "include/vector/operations/vector_operations.h"
#include "include/vector/value_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace expression {

class BinaryExpressionEvaluator : public PhysicalExpression {

public:
    BinaryExpressionEvaluator(unique_ptr<PhysicalExpression> leftExpr,
        unique_ptr<PhysicalExpression> rightExpr, unique_ptr<ValueVector> result,
        BINARY_OPERATION binaryOperation)
        : l{leftExpr->getResult()}, r{rightExpr->getResult()}, result{move(result)},
          leftExpr{move(leftExpr)}, rightExpr{move(rightExpr)},
          operation(VectorOperations::getOperation(binaryOperation)) {}

    void evaluate() override {
        leftExpr->evaluate();
        rightExpr->evaluate();
        operation(*l, *r, *result);
    }

    ValueVector* getResult() { return result.get(); }

private:
    ValueVector* l;
    ValueVector* r;
    unique_ptr<ValueVector> result;
    unique_ptr<PhysicalExpression> leftExpr;
    unique_ptr<PhysicalExpression> rightExpr;
    function<void(ValueVector&, ValueVector&, ValueVector&)> operation;
};

} // namespace expression
} // namespace graphflow
