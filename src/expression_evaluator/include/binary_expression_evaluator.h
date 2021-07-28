#pragma once

#include "src/common/include/vector/value_vector.h"
#include "src/expression_evaluator/include/expression_evaluator.h"

using namespace graphflow::common;

namespace graphflow {
namespace evaluator {

class BinaryExpressionEvaluator : public ExpressionEvaluator {

public:
    BinaryExpressionEvaluator(MemoryManager& memoryManager,
        unique_ptr<ExpressionEvaluator> leftExpr, unique_ptr<ExpressionEvaluator> rightExpr,
        ExpressionType expressionType, DataType dataType, bool isSelectOperation);

    void evaluate() override;
    uint64_t select(sel_t* selectedPositions) override;

    shared_ptr<ValueVector> createResultValueVector(MemoryManager& memoryManager);

private:
    function<void(ValueVector&, ValueVector&, ValueVector&)> executeOperation;
    function<uint64_t(ValueVector&, ValueVector&, sel_t*)> selectOperation;
};

} // namespace evaluator
} // namespace graphflow
