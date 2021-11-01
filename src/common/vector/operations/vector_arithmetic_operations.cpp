#include "src/common/include/vector/operations/vector_arithmetic_operations.h"

#include "src/common/include/operations/arithmetic_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

struct VectorArithmeticOperationExecutor {
public:
    template<class OP>
    static inline void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        switch (left.dataType) {
        case INT64: {
            switch (right.dataType) {
            case INT64: {
                if (std::is_same<OP, operation::Power>::value) {
                    assert(result.dataType == DOUBLE);
                    BinaryOperationExecutor::execute<int64_t, int64_t, double_t, OP,
                        false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                        left, right, result);
                } else {
                    BinaryOperationExecutor::execute<int64_t, int64_t, int64_t, OP,
                        false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                        left, right, result);
                }
            } break;
            case DOUBLE:
                BinaryOperationExecutor::execute<int64_t, double_t, double_t, OP,
                    false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                    left, right, result);
                break;
            default:
                assert(false);
            }
        } break;
        case DOUBLE: {
            switch (right.dataType) {
            case INT64:
                BinaryOperationExecutor::execute<double_t, int64_t, double_t, OP,
                    false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                    left, right, result);
                break;
            case DOUBLE:
                BinaryOperationExecutor::execute<double_t, double_t, double_t, OP,
                    false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                    left, right, result);
                break;
            default:
                assert(false);
            }
        } break;
        case STRING: {
            assert(right.dataType == STRING && (is_same<OP, operation::Add>::value));
            BinaryOperationExecutor::execute<gf_string_t, gf_string_t, gf_string_t, operation::Add,
                true /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(left, right, result);
        } break;
        case DATE: {
            switch (right.dataType) {
            case INT64: {
                if constexpr (is_same<OP, operation::Add>::value ||
                              is_same<OP, operation::Subtract>::value) {
                    BinaryOperationExecutor::execute<date_t, int64_t, date_t, OP,
                        false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                        left, right, result);
                } else {
                    assert(false);
                }
            } break;
            case INTERVAL: {
                if constexpr (is_same<OP, operation::Add>::value ||
                              is_same<OP, operation::Subtract>::value) {
                    BinaryOperationExecutor::execute<date_t, interval_t, date_t, OP,
                        false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                        left, right, result);
                } else {
                    assert(false);
                }
            } break;
            case DATE: {
                assert((is_same<OP, operation::Subtract>::value));
                BinaryOperationExecutor::execute<date_t, date_t, int64_t, operation::Subtract,
                    false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                    left, right, result);
            } break;
            default:
                assert(false);
            }
        } break;
        case TIMESTAMP: {
            switch (right.dataType) {
            case TIMESTAMP: {
                assert((is_same<OP, operation::Subtract>::value));
                BinaryOperationExecutor::execute<timestamp_t, timestamp_t, interval_t,
                    operation::Subtract, false /* IS_STRUCTURED_STRING */,
                    false /* IS_UNSTRUCTURED */>(left, right, result);
            } break;
            case INTERVAL: {
                if constexpr (is_same<OP, operation::Add>::value ||
                              is_same<OP, operation::Subtract>::value) {
                    BinaryOperationExecutor::execute<timestamp_t, interval_t, timestamp_t, OP,
                        false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                        left, right, result);
                } else {
                    assert(false);
                }
            } break;
            default:
                assert(false);
            }
        } break;
        case INTERVAL: {
            switch (right.dataType) {
            case INTERVAL: {
                if constexpr (is_same<OP, operation::Add>::value ||
                              is_same<OP, operation::Subtract>::value) {
                    BinaryOperationExecutor::execute<interval_t, interval_t, interval_t, OP,
                        false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                        left, right, result);
                } else {
                    assert(false);
                }
            } break;
            case INT64: {
                assert((is_same<OP, operation::Divide>::value));
                BinaryOperationExecutor::execute<interval_t, int64_t, interval_t, operation::Divide,
                    false /* IS_STRUCTURED_STRING */, false /* IS_UNSTRUCTURED */>(
                    left, right, result);
            } break;
            default:
                assert(false);
            }
        } break;
        case UNSTRUCTURED: {
            assert(right.dataType == UNSTRUCTURED);
            BinaryOperationExecutor::execute<Value, Value, Value, OP,
                false /* IS_STRUCTURED_STRING */, true /* IS_UNSTRUCTURED */>(left, right, result);
        } break;
        default:
            assert(false);
        }
    }

    template<class OP>
    static inline void execute(ValueVector& operand, ValueVector& result) {
        switch (operand.dataType) {
        case INT64: {
            UnaryOperationExecutor::execute<int64_t, int64_t, OP, false /* IS_NODE_ID */,
                true /* SKIP_NULL */>(operand, result);
        } break;
        case DOUBLE: {
            UnaryOperationExecutor::execute<double_t, double_t, OP, false /* IS_NODE_ID */,
                true /* SKIP_NULL */>(operand, result);
        } break;
        case UNSTRUCTURED: {
            UnaryOperationExecutor::execute<Value, Value, OP, false /* IS_NODE_ID */,
                true /* SKIP_NULL */>(operand, result);
        } break;
        default:
            assert(false);
        }
    }
};

void VectorArithmeticOperations::Add(ValueVector& left, ValueVector& right, ValueVector& result) {
    VectorArithmeticOperationExecutor::execute<operation::Add>(left, right, result);
}

void VectorArithmeticOperations::Subtract(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    VectorArithmeticOperationExecutor::execute<operation::Subtract>(left, right, result);
}

void VectorArithmeticOperations::Multiply(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    VectorArithmeticOperationExecutor::execute<operation::Multiply>(left, right, result);
}

void VectorArithmeticOperations::Divide(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    VectorArithmeticOperationExecutor::execute<operation::Divide>(left, right, result);
}

void VectorArithmeticOperations::Modulo(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    VectorArithmeticOperationExecutor::execute<operation::Modulo>(left, right, result);
}

void VectorArithmeticOperations::Power(ValueVector& left, ValueVector& right, ValueVector& result) {
    VectorArithmeticOperationExecutor::execute<operation::Power>(left, right, result);
}

void VectorArithmeticOperations::Negate(ValueVector& operand, ValueVector& result) {
    VectorArithmeticOperationExecutor::execute<operation::Negate>(operand, result);
}

void VectorArithmeticOperations::Abs(ValueVector& operand, ValueVector& result) {
    VectorArithmeticOperationExecutor::execute<operation::Abs>(operand, result);
}

void VectorArithmeticOperations::Floor(ValueVector& operand, ValueVector& result) {
    VectorArithmeticOperationExecutor::execute<operation::Floor>(operand, result);
}

void VectorArithmeticOperations::Ceil(ValueVector& operand, ValueVector& result) {
    VectorArithmeticOperationExecutor::execute<operation::Ceil>(operand, result);
}

} // namespace common
} // namespace graphflow
