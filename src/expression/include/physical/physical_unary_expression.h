#pragma once

#include "include/physical/physical_expression.h"

#include "src/common/include/vector/operations/vector_operations.h"
#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace expression {

class PhysicalUnaryExpression : public PhysicalExpression {

public:
    PhysicalUnaryExpression(unique_ptr<PhysicalExpression> child, UNARY_OPERATION operation)
        : operand{child->getResult()}, result{move(result)}, child{move(child)},
          operation(VectorOperations::getOperation(operation)) {}

    void evaluate() override {
        child->evaluate();
        operation(*operand, *result);
    }

protected:
    ValueVector* operand;
    unique_ptr<ValueVector> result;
    unique_ptr<PhysicalExpression> child;
    std::function<void(ValueVector&, ValueVector&)> operation;
};

} // namespace expression
} // namespace graphflow
