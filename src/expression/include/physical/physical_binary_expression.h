#pragma once

#include "src/common/include/vector/value_vector.h"
#include "src/expression/include/physical/physical_expression.h"

using namespace graphflow::common;

namespace graphflow {
namespace expression {

class PhysicalBinaryExpression : public PhysicalExpression {

public:
    PhysicalBinaryExpression(unique_ptr<PhysicalExpression> leftExpr,
        unique_ptr<PhysicalExpression> rightExpr, ExpressionType expressionType, DataType dataType);

    void evaluate() override;

    shared_ptr<ValueVector> createResultValueVector();

private:
    function<void(ValueVector&, ValueVector&, ValueVector&)> operation;
};

} // namespace expression
} // namespace graphflow
