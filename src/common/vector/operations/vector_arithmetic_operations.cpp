#include "src/common/include/vector/operations/vector_arithmetic_operations.h"

#include <cassert>

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
        case INT64:
            switch (right.dataType) {
            case INT64:
                if (std::is_same<OP, operation::Power>::value) {
                    BinaryOperationExecutor::executeArithmeticOps<int64_t, int64_t, double_t, OP,
                        false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                        left, right, result);
                } else {
                    BinaryOperationExecutor::executeArithmeticOps<int64_t, int64_t, int64_t, OP,
                        false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                        left, right, result);
                }
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeArithmeticOps<int64_t, double_t, double_t, OP,
                    false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                    left, right, result);
                break;
            default:
                assert(false);
            }
            break;
        case DOUBLE:
            switch (right.dataType) {
            case INT64:
                BinaryOperationExecutor::executeArithmeticOps<double_t, int64_t, double_t, OP,
                    false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                    left, right, result);
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeArithmeticOps<double_t, double_t, double_t, OP,
                    false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                    left, right, result);
                break;
            default:
                assert(false);
            }
            break;
        case UNSTRUCTURED:
            assert(right.dataType == UNSTRUCTURED);
            BinaryOperationExecutor::executeArithmeticOps<Value, Value, Value, OP,
                false /* IS_STRUCTURED_STRING */, true /* IS_UNSTRUCTURED */>(left, right, result);
            break;
        default:
            assert(false);
        }
    }

    template<class OP>
    static inline void execute(ValueVector& operand, ValueVector& result) {
        switch (operand.dataType) {
        case INT64:
            UnaryOperationExecutor::executeArithmeticOps<int64_t, int64_t, OP>(operand, result);
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
            operation::Add, true /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
            left, right, result);
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

void VectorArithmeticOperations::Abs(ValueVector& operand, ValueVector& result) {
    ArithmeticOperationExecutor::execute<operation::Abs>(operand, result);
}

} // namespace common
} // namespace graphflow
