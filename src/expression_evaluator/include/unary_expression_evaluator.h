#pragma once

#include "src/common/include/vector/value_vector.h"
#include "src/expression_evaluator/include/expression_evaluator.h"

namespace graphflow {
namespace evaluator {

class UnaryExpressionEvaluator : public ExpressionEvaluator {

public:
    UnaryExpressionEvaluator(MemoryManager& memoryManager, unique_ptr<ExpressionEvaluator> child,
        ExpressionType expressionType, DataType dataType);

    void evaluate() override;

    shared_ptr<ValueVector> createResultValueVector(MemoryManager& memoryManager);

protected:
    function<void(ValueVector&, ValueVector&)> operation;
};

} // namespace evaluator
} // namespace graphflow
