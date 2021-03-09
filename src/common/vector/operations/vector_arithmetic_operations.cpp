#include <stdexcept>

#include "src/common/include/operations/arithmetic_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"
#include "src/common/include/vector/operations/vector_operations.h"

namespace graphflow {
namespace common {

struct ArithmeticOperationExecutor {
public:
    template<class OP>
    static inline void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        switch (left.getDataType()) {
        case INT:
            switch (right.getDataType()) {
            case INT:
                if (std::is_same<OP, operation::Power>::value) {
                    BinaryOperationExecutor::execute<int32_t, int32_t, double_t, OP>(
                        left, right, result);
                } else {
                    BinaryOperationExecutor::execute<int32_t, int32_t, int32_t, OP>(
                        left, right, result);
                }
                break;
            case DOUBLE:
                BinaryOperationExecutor::execute<int32_t, double_t, double_t, OP>(
                    left, right, result);
                break;
            default:
                throw std::invalid_argument(
                    "Invalid or unsupported operand types for arithmetic operation.");
            }
            break;
        case DOUBLE:
            switch (right.getDataType()) {
            case INT:
                BinaryOperationExecutor::execute<double_t, int32_t, int32_t, OP>(
                    left, right, result);
                break;
            case DOUBLE:
                BinaryOperationExecutor::execute<double_t, double_t, double_t, OP>(
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
        case INT:
            UnaryOperationExecutor::execute<int32_t, int32_t, OP>(operand, result);
            break;
        case DOUBLE:
            UnaryOperationExecutor::execute<double_t, double_t, OP>(operand, result);
            break;
        default:
            throw std::invalid_argument(
                "Invalid or unsupported operand type for arithmetic operation.");
        }
    }
};

void VectorOperations::Add(ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Add>(left, right, result);
}

void VectorOperations::Subtract(ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Subtract>(left, right, result);
}

void VectorOperations::Multiply(ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Multiply>(left, right, result);
}

void VectorOperations::Divide(ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Divide>(left, right, result);
}

void VectorOperations::Modulo(ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Modulo>(left, right, result);
}

void VectorOperations::Power(ValueVector& left, ValueVector& right, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Power>(left, right, result);
}

void VectorOperations::Negate(ValueVector& operand, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Negate>(operand, result);
}

} // namespace common
} // namespace graphflow
