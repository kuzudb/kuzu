#include "src/expression_evaluator/include/expression_evaluator.h"

#include "src/common/include/vector/operations/vector_arithmetic_operations.h"
#include "src/common/include/vector/operations/vector_boolean_operations.h"
#include "src/common/include/vector/operations/vector_cast_operations.h"
#include "src/common/include/vector/operations/vector_comparison_operations.h"
#include "src/common/include/vector/operations/vector_node_id_operations.h"

namespace graphflow {
namespace evaluator {

std::function<void(ValueVector&, ValueVector&)> ExpressionEvaluator::getUnaryOperation(
    ExpressionType type) {
    switch (type) {
    case NOT:
        return VectorBooleanOperations::Not;
    case NEGATE:
        return VectorArithmeticOperations::Negate;
    case IS_NULL:
        return VectorComparisonOperations::IsNull;
    case IS_NOT_NULL:
        return VectorComparisonOperations::IsNotNull;
    case HASH_NODE_ID:
        return VectorNodeIDOperations::Hash;
    case DECOMPRESS_NODE_ID:
        return VectorNodeIDOperations::Decompress;
    case CAST_TO_STRING:
        return VectorCastOperations::castStructuredToStringValue;
    case CAST_TO_UNSTRUCTURED_VECTOR:
        return VectorCastOperations::castStructuredToUnstructuredValue;
    case CAST_UNSTRUCTURED_VECTOR_TO_BOOL_VECTOR:
        return VectorCastOperations::castUnstructuredToBoolValue;
    default:
        assert(false);
    }
}

std::function<void(ValueVector&, ValueVector&, ValueVector&)>
ExpressionEvaluator::getBinaryOperation(ExpressionType type) {
    switch (type) {
    case AND:
        return VectorBooleanOperations::And;
    case OR:
        return VectorBooleanOperations::Or;
    case XOR:
        return VectorBooleanOperations::Xor;
    case EQUALS:
        return VectorComparisonOperations::Equals;
    case NOT_EQUALS:
        return VectorComparisonOperations::NotEquals;
    case GREATER_THAN:
        return VectorComparisonOperations::GreaterThan;
    case GREATER_THAN_EQUALS:
        return VectorComparisonOperations::GreaterThanEquals;
    case LESS_THAN:
        return VectorComparisonOperations::LessThan;
    case LESS_THAN_EQUALS:
        return VectorComparisonOperations::LessThanEquals;
    case EQUALS_NODE_ID:
        return VectorNodeIDCompareOperations::Equals;
    case NOT_EQUALS_NODE_ID:
        return VectorNodeIDCompareOperations::NotEquals;
    case GREATER_THAN_NODE_ID:
        return VectorNodeIDCompareOperations::GreaterThan;
    case GREATER_THAN_EQUALS_NODE_ID:
        return VectorNodeIDCompareOperations::GreaterThanEquals;
    case LESS_THAN_NODE_ID:
        return VectorNodeIDCompareOperations::LessThan;
    case LESS_THAN_EQUALS_NODE_ID:
        return VectorNodeIDCompareOperations::LessThanEquals;
    case ADD:
        return VectorArithmeticOperations::Add;
    case SUBTRACT:
        return VectorArithmeticOperations::Subtract;
    case MULTIPLY:
        return VectorArithmeticOperations::Multiply;
    case DIVIDE:
        return VectorArithmeticOperations::Divide;
    case MODULO:
        return VectorArithmeticOperations::Modulo;
    case POWER:
        return VectorArithmeticOperations::Power;
    default:
        assert(false);
    }
}

bool ExpressionEvaluator::isResultFlat() {
    if (childrenExpr.empty()) {
        return result->state->isFlat();
    }
    for (auto& expr : childrenExpr) {
        if (!expr->isResultFlat()) {
            return false;
        }
    }
    return true;
}

} // namespace evaluator
} // namespace graphflow
