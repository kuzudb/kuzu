#include "src/common/include/vector/operations/vector_comparison_operations.h"

#include "src/common/include/operations/comparison_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

struct ComparisonOperationExecutor {
public:
    template<class OP>
    static inline void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        switch (left.dataType) {
        case BOOL: {
            assert(right.dataType == BOOL);
            BinaryOperationExecutor::execute<uint8_t, uint8_t, uint8_t, OP>(left, right, result);
        } break;
        case INT64: {
            switch (right.dataType) {
            case INT64: {
                BinaryOperationExecutor::execute<int64_t, int64_t, uint8_t, OP>(
                    left, right, result);
            } break;
            case DOUBLE: {
                BinaryOperationExecutor::execute<int64_t, double_t, uint8_t, OP>(
                    left, right, result);
            } break;
            default:
                assert(false);
            }
        } break;
        case DOUBLE: {
            switch (right.dataType) {
            case INT64: {
                BinaryOperationExecutor::execute<double_t, int64_t, uint8_t, OP>(
                    left, right, result);
            } break;
            case DOUBLE: {
                BinaryOperationExecutor::execute<double_t, double_t, uint8_t, OP>(
                    left, right, result);
            } break;
            default:
                assert(false);
            }
        } break;
        case STRING: {
            assert(right.dataType == STRING);
            BinaryOperationExecutor::execute<gf_string_t, gf_string_t, uint8_t, OP>(
                left, right, result);
        } break;
        case UNSTRUCTURED: {
            assert(right.dataType == UNSTRUCTURED);
            BinaryOperationExecutor::execute<Value, Value, uint8_t, OP>(left, right, result);
        } break;
        case DATE: {
            assert(right.dataType == DATE);
            BinaryOperationExecutor::execute<date_t, date_t, uint8_t, OP>(left, right, result);
        } break;
        case TIMESTAMP: {
            assert(right.dataType == TIMESTAMP);
            BinaryOperationExecutor::execute<timestamp_t, timestamp_t, uint8_t, OP>(
                left, right, result);
        } break;
        case INTERVAL: {
            assert(right.dataType == INTERVAL);
            BinaryOperationExecutor::execute<interval_t, interval_t, uint8_t, OP>(
                left, right, result);
        } break;
        case NODE: {
            assert(right.dataType == NODE);
            BinaryOperationExecutor::execute<nodeID_t, nodeID_t, uint8_t, OP>(left, right, result);
        } break;
        default:
            assert(false);
        }
    }

    template<class OP>
    static inline uint64_t select(ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
        switch (left.dataType) {
        case BOOL: {
            assert(right.dataType == BOOL);
            return BinaryOperationExecutor::select<uint8_t, uint8_t, uint8_t, OP>(
                left, right, selectedPositions);
        }
        case INT64: {
            switch (right.dataType) {
            case INT64:
                return BinaryOperationExecutor::select<int64_t, int64_t, uint8_t, OP>(
                    left, right, selectedPositions);
            case DOUBLE:
                return BinaryOperationExecutor::select<int64_t, double_t, uint8_t, OP>(
                    left, right, selectedPositions);
            default:
                assert(false);
            }
        } break;
        case DOUBLE: {
            switch (right.dataType) {
            case INT64:
                return BinaryOperationExecutor::select<double_t, int64_t, uint8_t, OP>(
                    left, right, selectedPositions);
            case DOUBLE:
                return BinaryOperationExecutor::select<double_t, double_t, uint8_t, OP>(
                    left, right, selectedPositions);
            default:
                assert(false);
            }
        } break;
        case STRING: {
            assert(right.dataType == STRING);
            return BinaryOperationExecutor::select<gf_string_t, gf_string_t, uint8_t, OP>(
                left, right, selectedPositions);
        }
        case UNSTRUCTURED: {
            assert(right.dataType == UNSTRUCTURED);
            return BinaryOperationExecutor::select<Value, Value, uint8_t, OP>(
                left, right, selectedPositions);
        }
        case DATE: {
            assert(right.dataType == DATE);
            return BinaryOperationExecutor::select<date_t, date_t, uint8_t, OP>(
                left, right, selectedPositions);
        }
        case TIMESTAMP: {
            assert(right.dataType == TIMESTAMP);
            return BinaryOperationExecutor::select<timestamp_t, timestamp_t, uint8_t, OP>(
                left, right, selectedPositions);
        }
        case INTERVAL: {
            assert(right.dataType == INTERVAL);
            return BinaryOperationExecutor::select<interval_t, interval_t, uint8_t, OP>(
                left, right, selectedPositions);
        }
        case NODE: {
            assert(right.dataType == NODE);
            return BinaryOperationExecutor::select<nodeID_t, nodeID_t, uint8_t, OP>(
                left, right, selectedPositions);
        }
        default:
            assert(false);
        }
    }
};

void VectorComparisonOperations::Equals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::Equals>(left, right, result);
}

void VectorComparisonOperations::NotEquals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::NotEquals>(left, right, result);
}

void VectorComparisonOperations::GreaterThan(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::GreaterThan>(left, right, result);
}

void VectorComparisonOperations::GreaterThanEquals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::GreaterThanEquals>(left, right, result);
}

void VectorComparisonOperations::LessThan(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::LessThan>(left, right, result);
}

void VectorComparisonOperations::LessThanEquals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::LessThanEquals>(left, right, result);
}

void VectorComparisonOperations::IsNull(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::execute<uint8_t, uint8_t, operation::IsNull>(operand, result);
}

void VectorComparisonOperations::IsNotNull(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::execute<uint8_t, uint8_t, operation::IsNotNull>(operand, result);
}

uint64_t VectorComparisonOperations::EqualsSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return ComparisonOperationExecutor::select<operation::Equals>(left, right, selectedPositions);
}

uint64_t VectorComparisonOperations::NotEqualsSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return ComparisonOperationExecutor::select<operation::NotEquals>(
        left, right, selectedPositions);
}

uint64_t VectorComparisonOperations::GreaterThanSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return ComparisonOperationExecutor::select<operation::GreaterThan>(
        left, right, selectedPositions);
}

uint64_t VectorComparisonOperations::GreaterThanEqualsSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return ComparisonOperationExecutor::select<operation::GreaterThanEquals>(
        left, right, selectedPositions);
}

uint64_t VectorComparisonOperations::LessThanSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return ComparisonOperationExecutor::select<operation::LessThan>(left, right, selectedPositions);
}

uint64_t VectorComparisonOperations::LessThanEqualsSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return ComparisonOperationExecutor::select<operation::LessThanEquals>(
        left, right, selectedPositions);
}

uint64_t VectorComparisonOperations::IsNullSelect(ValueVector& operand, sel_t* selectedPositions) {
    return UnaryOperationExecutor::select<operation::IsNull>(operand, selectedPositions);
}

uint64_t VectorComparisonOperations::IsNotNullSelect(
    ValueVector& operand, sel_t* selectedPositions) {
    return UnaryOperationExecutor::select<operation::IsNotNull>(operand, selectedPositions);
}

} // namespace common
} // namespace graphflow
