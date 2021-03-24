#pragma once

#include "src/common/include/vector/value_vector.h"
#include "src/expression/include/physical/physical_expression.h"

namespace graphflow {
namespace expression {

class PhysicalUnaryExpression : public PhysicalExpression {

public:
    PhysicalUnaryExpression(unique_ptr<PhysicalExpression> child, ExpressionType expressionType);

    void evaluate() override;

protected:
    function<void(ValueVector&, ValueVector&)> operation;
};

} // namespace expression
} // namespace graphflow
