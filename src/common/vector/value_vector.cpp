#include "src/common/include/vector/value_vector.h"

#include "src/common/include/operations/comparison_operations.h"
#include "src/common/include/vector/operations/vector_arithmetic_operations.h"
#include "src/common/include/vector/operations/vector_boolean_operations.h"
#include "src/common/include/vector/operations/vector_comparison_operations.h"
#include "src/common/include/vector/operations/vector_node_id_operations.h"

using namespace graphflow::common::operation;

namespace graphflow {
namespace common {

uint8_t* ValueVector::reserve(size_t capacity) {
    if (this->capacity < capacity) {
        auto newBuffer = new uint8_t[capacity];
        memcpy(newBuffer, buffer.get(), this->capacity);
        buffer.reset(newBuffer);
        this->capacity = capacity;
    }
    return buffer.get();
}

std::function<void(ValueVector&, ValueVector&)> ValueVector::getUnaryOperation(
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
    default:
        throw std::invalid_argument("Invalid or unsupported unary expression.");
    }
}

std::function<void(ValueVector&, ValueVector&, ValueVector&)> ValueVector::getBinaryOperation(
    ExpressionType type) {
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
        throw std::invalid_argument("Invalid or unsupported binary expression.");
    }
}

template<class T, class FUNC = std::function<uint8_t(T)>>
static void fillOperandNullMask(ValueVector& operand) {
    auto values = (T*)operand.getValues();
    auto nullMask = operand.getNullMask();
    if (operand.isFlat()) {
        nullMask[operand.getCurrPos()] =
            IsNull::operation(values[operand.owner->getCurrSelectedValuesPos()]);
    } else {
        auto selectedValuesPos = operand.getSelectedValuesPos();
        auto size = operand.getNumSelectedValues();
        for (uint64_t i = 0; i < size; i++) {
            nullMask[i] = IsNull::operation(values[selectedValuesPos[i]]);
        }
    }
}

void ValueVector::fillNullMask() {
    switch (dataType) {
    case BOOL:
        fillOperandNullMask<uint8_t>(*this);
        break;
    case INT32:
        fillOperandNullMask<int32_t>(*this);
        break;
    case DOUBLE:
        fillOperandNullMask<double_t>(*this);
        break;
    default:
        throw std::invalid_argument("Invalid or unsupported type for comparison.");
    }
}

} // namespace common
} // namespace graphflow
