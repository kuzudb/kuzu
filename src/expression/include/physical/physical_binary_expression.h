#pragma once

#include "src/common/include/vector/value_vector.h"
#include "src/expression/include/physical/physical_expression.h"

using namespace graphflow::common;

namespace graphflow {
namespace expression {

class PhysicalBinaryExpression : public PhysicalExpression {

public:
    PhysicalBinaryExpression(unique_ptr<PhysicalExpression> leftExpr,
        unique_ptr<PhysicalExpression> rightExpr, ExpressionType expressionType);

    void evaluate() override;

    void setExpressionInputDataChunk(shared_ptr<DataChunk> dataChunk) override;
    void setExpressionResultOwnerState(shared_ptr<DataChunkState> dataChunkState) override;

private:
    function<void(ValueVector&, ValueVector&, ValueVector&)> operation;
};

} // namespace expression
} // namespace graphflow
