#include "src/common/include/vector/operations/vector_arithmetic_operations.h"

#include <stdexcept>

#include "src/common/include/operations/arithmetic_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

struct ArithmeticOperationExecutor {
public:
    template<class OP>
    static inline void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        switch (left.getDataType()) {
        case INT32:
            switch (right.getDataType()) {
            case INT32:
                if (std::is_same<OP, operation::Power>::value) {
                    BinaryOperationExecutor::executeNonBoolOp<int32_t, int32_t, double_t, OP>(
                        left, right, result);
                } else {
                    BinaryOperationExecutor::executeNonBoolOp<int32_t, int32_t, int32_t, OP>(
                        left, right, result);
                }
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeNonBoolOp<int32_t, double_t, double_t, OP>(
                    left, right, result);
                break;
            default:
                throw std::invalid_argument(
                    "Invalid or unsupported operand types for arithmetic operation.");
            }
            break;
        case DOUBLE:
            switch (right.getDataType()) {
            case INT32:
                BinaryOperationExecutor::executeNonBoolOp<double_t, int32_t, int32_t, OP>(
                    left, right, result);
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeNonBoolOp<double_t, double_t, double_t, OP>(
                    left, right, result);
                break;
            default:
                throw std::invalid_argument(
                    "Invalid or unsupported operand types for arithmetic operation.");
            }
            break;
        default:
            throw std::invalid_argument(
                "Invalid or unsupported operand types for arithmetic operation.");
        }
    }

    template<class OP>
    static inline void execute(ValueVector& operand, ValueVector& result) {
        switch (operand.getDataType()) {
        case INT32:
            UnaryOperationExecutor::executeNonBoolOp<int32_t, int32_t, OP>(operand, result);
            break;
        case DOUBLE:
            UnaryOperationExecutor::executeNonBoolOp<double_t, double_t, OP>(operand, result);
            break;
        default:
            throw std::invalid_argument(
                "Invalid or unsupported operand type for arithmetic operation.");
        }
    }
};

void VectorArithmeticOperations::Add(ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Add>(left, right, result);
}

void VectorArithmeticOperations::Subtract(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Subtract>(left, right, result);
}

void VectorArithmeticOperations::Multiply(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Multiply>(left, right, result);
}

void VectorArithmeticOperations::Divide(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Divide>(left, right, result);
}

void VectorArithmeticOperations::Modulo(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Modulo>(left, right, result);
}

void VectorArithmeticOperations::Power(ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Power>(left, right, result);
}

void VectorArithmeticOperations::Negate(ValueVector& operand, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Negate>(operand, result);
}

} // namespace common
} // namespace graphflow
