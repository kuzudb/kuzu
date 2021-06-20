#include "src/common/include/vector/operations/vector_arithmetic_operations.h"

#include <cassert>
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
        switch (left.dataType) {
        case INT32:
            switch (right.dataType) {
            case INT32:
                if (std::is_same<OP, operation::Power>::value) {
                    BinaryOperationExecutor::executeArithmeticOps<int32_t, int32_t, double_t, OP,
                        false /* ALLOCATE_STRING_OVERFLOW */>(left, right, result);
                } else {
                    BinaryOperationExecutor::executeArithmeticOps<int32_t, int32_t, int32_t, OP,
                        false /* ALLOCATE_STRING_OVERFLOW */>(left, right, result);
                }
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeArithmeticOps<int32_t, double_t, double_t, OP,
                    false /* ALLOCATE_STRING_OVERFLOW */>(left, right, result);
                break;
            default:
                assert(false);
            }
            break;
        case DOUBLE:
            switch (right.dataType) {
            case INT32:
                BinaryOperationExecutor::executeArithmeticOps<double_t, int32_t, double_t, OP,
                    false /* ALLOCATE_STRING_OVERFLOW */>(left, right, result);
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeArithmeticOps<double_t, double_t, double_t, OP,
                    false /* ALLOCATE_STRING_OVERFLOW */>(left, right, result);
                break;
            default:
                assert(false);
            }
            break;
        case UNSTRUCTURED:
            assert(right.dataType == UNSTRUCTURED);
            BinaryOperationExecutor::executeArithmeticOps<Value, Value, Value, OP,
                false /* ALLOCATE_STRING_OVERFLOW */>(left, right, result);
            break;
        default:
            assert(false);
        }
    }

    template<class OP>
    static inline void execute(ValueVector& operand, ValueVector& result) {
        switch (operand.dataType) {
        case INT32:
            UnaryOperationExecutor::executeArithmeticOps<int32_t, int32_t, OP>(operand, result);
            break;
        case DOUBLE:
            UnaryOperationExecutor::executeArithmeticOps<double_t, double_t, OP>(operand, result);
            break;
        case UNSTRUCTURED:
            UnaryOperationExecutor::executeArithmeticOps<Value, Value, OP>(operand, result);
            break;
        default:
            assert(false);
        }
    }
};

void VectorArithmeticOperations::Add(ValueVector& left, ValueVector& right, ValueVector& result) {
    if (left.dataType == STRING) {
        assert(right.dataType == STRING);
        BinaryOperationExecutor::executeArithmeticOps<gf_string_t, gf_string_t, gf_string_t,
            operation::Add, true /* ALLOCATE_STRING_OVERFLOW */>(left, right, result);
    } else {
        ArithmeticOperationExecutor::execute<operation::Add>(left, right, result);
    }
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
