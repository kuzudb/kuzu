#include "src/common/include/vector/operations/vector_comparison_operations.h"

#include <stdexcept>

#include "src/common/include/operations/comparison_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

void throwException(DataType lDataType, DataType rDataType) {
    throw invalid_argument(DataTypeNames[lDataType] + "left operand compared with " +
                           DataTypeNames[rDataType] + " right operand should return false. " +
                           "Not supported for now.");
}

struct ComparisonOperationExecutor {
public:
    template<class OP>
    static inline void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        switch (left.dataType) {
        case BOOL:
            switch (right.dataType) {
            case BOOL:
                BinaryOperationExecutor::executeArithmeticAndComparisonOperations<uint8_t, uint8_t,
                    uint8_t, OP>(left, right, result);
                break;
            default:
                throwException(left.dataType, right.dataType);
            }
            break;
        case INT32:
            switch (right.dataType) {
            case INT32:
                BinaryOperationExecutor::executeArithmeticAndComparisonOperations<int32_t, int32_t,
                    uint8_t, OP>(left, right, result);
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeArithmeticAndComparisonOperations<int32_t, double_t,
                    uint8_t, OP>(left, right, result);
                break;
            default:
                throwException(left.dataType, right.dataType);
            }
            break;
        case DOUBLE:
            switch (right.dataType) {
            case INT32:
                BinaryOperationExecutor::executeArithmeticAndComparisonOperations<double_t, int32_t,
                    uint8_t, OP>(left, right, result);
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeArithmeticAndComparisonOperations<double_t,
                    double_t, uint8_t, OP>(left, right, result);
                break;
            default:
                throwException(left.dataType, right.dataType);
            }
            break;
        case STRING:
            switch (right.dataType) {
            case STRING:
                BinaryOperationExecutor::executeArithmeticAndComparisonOperations<gf_string_t,
                    gf_string_t, uint8_t, OP>(left, right, result);
                break;
            default:
                throwException(left.dataType, right.dataType);
            }
            break;
        case UNKNOWN:
            switch (right.dataType) {
            case UNKNOWN:
                BinaryOperationExecutor::executeArithmeticAndComparisonOperations<Value, Value,
                    uint8_t, OP>(left, right, result);
                break;
            default:
                assert(false); // Should never happen as we should always cast.
            }
            break;
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
    UnaryOperationExecutor::nullMaskCmp<true /* NULL */>(operand, result);
}

void VectorComparisonOperations::IsNotNull(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::nullMaskCmp<false /* NULL */>(operand, result);
}

} // namespace common
} // namespace graphflow
