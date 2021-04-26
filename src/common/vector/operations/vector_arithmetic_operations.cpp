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
                    BinaryOperationExecutor::executeArithmeticOpsOnPrimitivesAndComparisonOps<
                        int32_t, int32_t, double_t, OP>(left, right, result);
                } else {
                    BinaryOperationExecutor::executeArithmeticOpsOnPrimitivesAndComparisonOps<
                        int32_t, int32_t, int32_t, OP>(left, right, result);
                }
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeArithmeticOpsOnPrimitivesAndComparisonOps<int32_t,
                    double_t, double_t, OP>(left, right, result);
                break;
            default:
                assert(false);
            }
            break;
        case DOUBLE:
            switch (right.dataType) {
            case INT32:
                BinaryOperationExecutor::executeArithmeticOpsOnPrimitivesAndComparisonOps<double_t,
                    int32_t, int32_t, OP>(left, right, result);
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeArithmeticOpsOnPrimitivesAndComparisonOps<double_t,
                    double_t, double_t, OP>(left, right, result);
                break;
            default:
                assert(false);
            }
            break;
        default:
            assert(false);
        }
    }

    template<class OP>
    static inline void execute(ValueVector& operand, ValueVector& result) {
        switch (operand.dataType) {
        case INT32:
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

void VectorArithmeticOperations::Add(ValueVector& left, ValueVector& right, ValueVector& result) {
    switch (left.dataType) {
    case UNSTRUCTURED:
        assert(right.dataType == UNSTRUCTURED);
        BinaryOperationExecutor::executeArithmeticOpsOnObjects<Value, operation::AddOrConcatValues>(
            left, right, result);
        break;
    case STRING:
        // Add takes both operands as STRING or neither of the operands is STRING.
        // An Add between two STRING(s) is interpreted as Concat.
        assert(left.dataType == STRING && right.dataType == STRING);
        BinaryOperationExecutor::executeArithmeticOpsOnObjects<gf_string_t,
            operation::ConcatStrings>(left, right, result);
        break;
    default:
        ArithmeticOperationExecutor::execute<operation::Add>(left, right, result);
    }
}

void VectorArithmeticOperations::Subtract(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    if (left.dataType == UNSTRUCTURED) {
        assert(right.dataType == UNSTRUCTURED);
        BinaryOperationExecutor::executeArithmeticOpsOnObjects<Value, operation::SubtractValues>(
            left, right, result);
    } else {
        assert(right.dataType != UNSTRUCTURED);
        ArithmeticOperationExecutor::execute<operation::Subtract>(left, right, result);
    }
}

void VectorArithmeticOperations::Multiply(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    if (left.dataType == UNSTRUCTURED) {
        assert(right.dataType == UNSTRUCTURED);
        BinaryOperationExecutor::executeArithmeticOpsOnObjects<Value, operation::MultiplyValues>(
            left, right, result);
    } else {
        assert(right.dataType != UNSTRUCTURED);
        ArithmeticOperationExecutor::execute<operation::Multiply>(left, right, result);
    }
}

void VectorArithmeticOperations::Divide(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    if (left.dataType == UNSTRUCTURED) {
        assert(right.dataType == UNSTRUCTURED);
        BinaryOperationExecutor::executeArithmeticOpsOnObjects<Value, operation::DivideValues>(
            left, right, result);
    } else {
        assert(right.dataType != UNSTRUCTURED);
        ArithmeticOperationExecutor::execute<operation::Divide>(left, right, result);
    }
}

void VectorArithmeticOperations::Modulo(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    if (left.dataType == UNSTRUCTURED) {
        assert(right.dataType == UNSTRUCTURED);
        BinaryOperationExecutor::executeArithmeticOpsOnObjects<Value, operation::ModuloValues>(
            left, right, result);
    } else {
        assert(right.dataType != UNSTRUCTURED);
        ArithmeticOperationExecutor::execute<operation::Modulo>(left, right, result);
    }
}

void VectorArithmeticOperations::Power(ValueVector& left, ValueVector& right, ValueVector& result) {
    if (left.dataType == UNSTRUCTURED) {
        assert(right.dataType == UNSTRUCTURED);
        BinaryOperationExecutor::executeArithmeticOpsOnObjects<Value, operation::PowerValues>(
            left, right, result);
    } else {
        assert(right.dataType != UNSTRUCTURED);
        ArithmeticOperationExecutor::execute<operation::Power>(left, right, result);
    }
}

void VectorArithmeticOperations::Negate(ValueVector& operand, ValueVector& result) {
    if (operand.dataType == UNSTRUCTURED) {
        assert(false); // TODO: implement negate.
    } else {
        ArithmeticOperationExecutor::execute<operation::Negate>(operand, result);
    }
}

} // namespace common
} // namespace graphflow
