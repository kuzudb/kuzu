#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

enum UNARY_OPERATION {
    /* boolean operations */
    NOT,
    /* arithmetic operations */
    NEGATE
};

enum BINARY_OPERATION {
    AND,
    OR,
    XOR,
    /* comparison operations */
    EQUALS,
    NOT_EQUALS,
    GREATER_THAN,
    GREATER_THAN_EQUALS,
    LESS_THAN,
    LESS_THAN_EQUALS,
    /* arithmetic operations */
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    MODULO,
    POWER,
};

struct VectorOperations {

    static std::function<void(ValueVector&, ValueVector&)> getOperation(UNARY_OPERATION type) {
        switch (type) {
        case NOT:
            return VectorOperations::Not;
        case NEGATE:
            return VectorOperations::Negate;
        default:
            throw std::invalid_argument("Invalid or unsupported unary expression.");
        }
    }

    static std::function<void(ValueVector&, ValueVector&, ValueVector&)> getOperation(
        BINARY_OPERATION type) {
        switch (type) {
        case AND:
            return VectorOperations::And;
        case OR:
            return VectorOperations::Or;
        case XOR:
            return VectorOperations::Xor;
        case EQUALS:
            return VectorOperations::Equals;
        case NOT_EQUALS:
            return VectorOperations::NotEquals;
        case GREATER_THAN:
            return VectorOperations::GreaterThan;
        case GREATER_THAN_EQUALS:
            return VectorOperations::GreaterThanEquals;
        case LESS_THAN:
            return VectorOperations::LessThan;
        case LESS_THAN_EQUALS:
            return VectorOperations::LessThanEquals;
        case ADD:
            return VectorOperations::Add;
        case SUBTRACT:
            return VectorOperations::Subtract;
        case MULTIPLY:
            return VectorOperations::Multiply;
        case DIVIDE:
            return VectorOperations::Divide;
        case MODULO:
            return VectorOperations::Modulo;
        case POWER:
            return VectorOperations::Power;
        default:
            throw std::invalid_argument("Invalid or unsupported binary expression.");
        }
    }

    /***
     * Vector Arithmetic Operations.
     **/

    // result = left + right
    static void Add(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left - right
    static void Subtract(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left * right
    static void Multiply(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left / right
    static void Divide(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left % right
    static void Modulo(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left ^ right
    static void Power(ValueVector& left, ValueVector& right, ValueVector& result);
    // for pos, element in vector: result[pos] = -element
    static void Negate(ValueVector& operand, ValueVector& result);

    /***
     * Vector Comparison Operations.
     **/

    // result = left == right
    static void Equals(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left != right
    static void NotEquals(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left > right
    static void GreaterThan(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left >= right
    static void GreaterThanEquals(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left < right
    static void LessThan(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left <= right
    static void LessThanEquals(ValueVector& left, ValueVector& right, ValueVector& result);

    /***
     * Vector Boolean Operations.
     **/
    // result = left && right
    static void And(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left || right
    static void Or(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = left ^ right
    static void Xor(ValueVector& left, ValueVector& right, ValueVector& result);
    // result = !vector
    static void Not(ValueVector& operand, ValueVector& result);
};

} // namespace common
} // namespace graphflow
