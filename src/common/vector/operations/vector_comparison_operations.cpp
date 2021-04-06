#include "src/common/include/vector/operations/vector_comparison_operations.h"

#include <stdexcept>

#include "src/common/include/operations/comparison_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

struct ComparisonOperationExecutor {
public:
    template<class OP>
    static inline void execute(ValueVector& left, ValueVector& right, ValueVector& result) {
        if (left.getDataType() != right.getDataType()) {
            throw std::invalid_argument("Different data types cannot be compared.");
        }
        switch (left.getDataType()) {
        case BOOL:
            compare<uint8_t, OP>(left, right, result);
            break;
        case INT32:
            compare<int32_t, OP>(left, right, result);
            break;
        case DOUBLE:
            compare<double_t, OP>(left, right, result);
            break;
        case STRING:
            compare<gf_string_t, OP>(left, right, result);
            break;
        default:
            throw std::invalid_argument("Invalid or unsupported type for comparison.");
        }
    }

private:
    template<class T, class OP>
    static inline void compare(ValueVector& left, ValueVector& right, ValueVector& result) {
        BinaryOperationExecutor::executeNonBoolOp<T, T, uint8_t, OP>(left, right, result);
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
