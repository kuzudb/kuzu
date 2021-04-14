#pragma once

#include "src/common/include/vector/value_vector.h"
#include "src/expression/include/physical/physical_expression.h"

namespace graphflow {
namespace expression {

class PhysicalUnaryExpression : public PhysicalExpression {

public:
    PhysicalUnaryExpression(unique_ptr<PhysicalExpression> child, ExpressionType expressionType);

    void evaluate() override;

    void setExpressionInputDataChunk(shared_ptr<DataChunk> dataChunk) override;
    void setExpressionResultOwnerState(shared_ptr<DataChunkState> dataChunkState) override;

protected:
    function<void(ValueVector&, ValueVector&)> operation;
};

} // namespace expression
} // namespace graphflow
