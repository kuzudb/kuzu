#include "src/expression_evaluator/include/expression_evaluator.h"

#include "src/common/include/interval.h"
#include "src/common/include/vector/operations/vector_arithmetic_operations.h"
#include "src/common/include/vector/operations/vector_boolean_operations.h"
#include "src/common/include/vector/operations/vector_cast_operations.h"
#include "src/common/include/vector/operations/vector_comparison_operations.h"
#include "src/common/include/vector/operations/vector_hash_operations.h"

namespace graphflow {
namespace evaluator {

std::function<void(ValueVector&, ValueVector&)> ExpressionEvaluator::getUnaryVectorExecuteOperation(
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
        return VectorHashOperations::Hash;
    case CAST_TO_STRING:
        return VectorCastOperations::castStructuredToString;
    case CAST_TO_UNSTRUCTURED_VALUE:
        return VectorCastOperations::castStructuredToUnstructuredValue;
    case CAST_UNSTRUCTURED_TO_BOOL_VALUE:
        return VectorCastOperations::castUnstructuredToBoolValue;
    case CAST_STRING_TO_DATE:
        return VectorCastOperations::castStringToDate;
    case CAST_STRING_TO_TIMESTAMP:
        return VectorCastOperations::castStringToTimestamp;
    case CAST_STRING_TO_INTERVAL:
        return VectorCastOperations::castStringToInterval;
    case ABS_FUNC:
        return VectorArithmeticOperations::Abs;
    case FLOOR_FUNC:
        return VectorArithmeticOperations::Floor;
    case CEIL_FUNC:
        return VectorArithmeticOperations::Ceil;
    default:
        throw invalid_argument("Unsupported expression type: " + expressionTypeToString(type));
    }
}

function<uint64_t(ValueVector&, sel_t*)> ExpressionEvaluator::getUnaryVectorSelectOperation(
    ExpressionType type) {
    switch (type) {
    case NOT:
        return VectorBooleanOperations::NotSelect;
    case IS_NULL:
        return VectorComparisonOperations::IsNullSelect;
    case IS_NOT_NULL:
        return VectorComparisonOperations::IsNotNullSelect;
    default:
        throw invalid_argument("Unsupported expression type: " + expressionTypeToString(type));
    }
}

std::function<void(ValueVector&, ValueVector&, ValueVector&)>
ExpressionEvaluator::getBinaryVectorExecuteOperation(ExpressionType type) {
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
        return VectorComparisonOperations::Equals;
    case NOT_EQUALS_NODE_ID:
        return VectorComparisonOperations::NotEquals;
    case GREATER_THAN_NODE_ID:
        return VectorComparisonOperations::GreaterThan;
    case GREATER_THAN_EQUALS_NODE_ID:
        return VectorComparisonOperations::GreaterThanEquals;
    case LESS_THAN_NODE_ID:
        return VectorComparisonOperations::LessThan;
    case LESS_THAN_EQUALS_NODE_ID:
        return VectorComparisonOperations::LessThanEquals;
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
        throw invalid_argument("Unsupported expression type: " + expressionTypeToString(type));
    }
}

std::function<uint64_t(ValueVector&, ValueVector&, sel_t*)>
ExpressionEvaluator::getBinaryVectorSelectOperation(ExpressionType type) {
    switch (type) {
    case AND:
        return VectorBooleanOperations::AndSelect;
    case OR:
        return VectorBooleanOperations::OrSelect;
    case XOR:
        return VectorBooleanOperations::XorSelect;
    case EQUALS:
        return VectorComparisonOperations::EqualsSelect;
    case NOT_EQUALS:
        return VectorComparisonOperations::NotEqualsSelect;
    case GREATER_THAN:
        return VectorComparisonOperations::GreaterThanSelect;
    case GREATER_THAN_EQUALS:
        return VectorComparisonOperations::GreaterThanEqualsSelect;
    case LESS_THAN:
        return VectorComparisonOperations::LessThanSelect;
    case LESS_THAN_EQUALS:
        return VectorComparisonOperations::LessThanEqualsSelect;
    case EQUALS_NODE_ID:
        return VectorComparisonOperations::EqualsSelect;
    case NOT_EQUALS_NODE_ID:
        return VectorComparisonOperations::NotEqualsSelect;
    case GREATER_THAN_NODE_ID:
        return VectorComparisonOperations::GreaterThanSelect;
    case GREATER_THAN_EQUALS_NODE_ID:
        return VectorComparisonOperations::GreaterThanEqualsSelect;
    case LESS_THAN_NODE_ID:
        return VectorComparisonOperations::LessThanSelect;
    case LESS_THAN_EQUALS_NODE_ID:
        return VectorComparisonOperations::LessThanEqualsSelect;
    default:
        throw invalid_argument("Unsupported expression type: " + expressionTypeToString(type));
    }
}

void ExpressionEvaluator::initResultSet(const ResultSet& resultSet, MemoryManager& memoryManager) {
    if (dataPos.dataChunkPos != UINT32_MAX && dataPos.valueVectorPos != UINT32_MAX) {
        result = resultSet.dataChunks[dataPos.dataChunkPos]->valueVectors[dataPos.valueVectorPos];
    }
}

// Select function for leaf expressions, for the expression's return data type is boolean.
uint64_t ExpressionEvaluator::select(sel_t* selectedPositions) {
    assert(dataType == BOOL);
    uint64_t numSelectedValues = 0;
    if (result->state->isFlat()) {
        auto pos = result->state->getPositionOfCurrIdx();
        numSelectedValues += (result->values[pos] == TRUE) && (!result->isNull(pos));
    } else {
        for (auto i = 0u; i < result->state->selectedSize; i++) {
            auto pos = result->state->selectedPositions[i];
            selectedPositions[numSelectedValues] = pos;
            numSelectedValues += (result->values[pos] == TRUE) && (!result->isNull(pos));
        }
    }
    return numSelectedValues;
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

unique_ptr<ExpressionEvaluator> ExpressionEvaluator::clone() {
    if (isExpressionLiteral(expressionType)) {
        return make_unique<ExpressionEvaluator>(result, expressionType);
    } else {
        // property expression evaluator clone
        assert(childrenExpr.empty());
        return make_unique<ExpressionEvaluator>(dataPos, expressionType, dataType);
    }
}

} // namespace evaluator
} // namespace graphflow
