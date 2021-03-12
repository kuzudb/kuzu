#include "src/common/include/vector/value_vector.h"

#include "src/common/include/vector/operations/vector_arithmetic_operations.h"
#include "src/common/include/vector/operations/vector_boolean_operations.h"
#include "src/common/include/vector/operations/vector_comparison_operations.h"

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

} // namespace common
} // namespace graphflow