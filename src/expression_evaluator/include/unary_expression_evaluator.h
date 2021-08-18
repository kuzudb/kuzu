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

    uint64_t select(sel_t* selectedPositions) override;

    shared_ptr<ValueVector> createResultValueVector(MemoryManager& memoryManager);

    unique_ptr<ExpressionEvaluator> clone(
        MemoryManager& memoryManager, const ResultSet& resultSet) override;

protected:
    function<void(ValueVector&, ValueVector&)> executeOperation;
    function<uint64_t(ValueVector&, sel_t*)> selectOperation;
};

} // namespace evaluator
} // namespace graphflow
