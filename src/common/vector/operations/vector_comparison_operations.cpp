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
        switch (left.dataType) {
        case BOOL:
            switch (right.dataType) {
            case BOOL:
                BinaryOperationExecutor::executeComparisonOps<uint8_t, uint8_t, OP>(
                    left, right, result);
                break;
            default:
                assert(false);
            }
            break;
        case INT32:
            switch (right.dataType) {
            case INT32:
                BinaryOperationExecutor::executeComparisonOps<int32_t, int32_t, OP>(
                    left, right, result);
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeComparisonOps<int32_t, double_t, OP>(
                    left, right, result);
                break;
            default:
                assert(false);
            }
            break;
        case DOUBLE:
            switch (right.dataType) {
            case INT32:
                BinaryOperationExecutor::executeComparisonOps<double_t, int32_t, OP>(
                    left, right, result);
                break;
            case DOUBLE:
                BinaryOperationExecutor::executeComparisonOps<double_t, double_t, OP>(
                    left, right, result);
                break;
            default:
                assert(false);
            }
            break;
        case STRING:
            switch (right.dataType) {
            case STRING:
                BinaryOperationExecutor::executeComparisonOps<gf_string_t, gf_string_t, OP>(
                    left, right, result);
                break;
            default:
                assert(false);
            }
            break;
        case UNSTRUCTURED:
            switch (right.dataType) {
            case UNSTRUCTURED:
                BinaryOperationExecutor::executeComparisonOps<Value, Value, OP>(
                    left, right, result);
                break;
            default:
                assert(false);
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
