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

    void initResultSet(const ResultSet& resultSet, MemoryManager* memoryManager) override;

    void evaluate() override;
    uint64_t select(sel_t* selectedPositions) override;

    shared_ptr<ValueVector> createResultValueVector(MemoryManager* memoryManager);

    unique_ptr<ExpressionEvaluator> clone() override;

private:
    std::function<void(ValueVector&, ValueVector&, ValueVector&)> executeOperation;
    std::function<uint64_t(ValueVector&, ValueVector&, sel_t*)> selectOperation;
};

} // namespace evaluator
} // namespace graphflow
