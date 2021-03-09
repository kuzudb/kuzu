#include <stdexcept>

#include "src/common/include/operations/comparison_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/vector_operations.h"

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
        case INT:
            compare<int32_t, OP>(left, right, result);
            break;
        case DOUBLE:
            compare<double_t, OP>(left, right, result);
            break;
        /* TODO: add gf_string_t comparison. */
        default:
            throw std::invalid_argument("Invalid or unsupported type for comparison.");
        }
    }

private:
    template<class T, class OP>
    static inline void compare(ValueVector& left, ValueVector& right, ValueVector& result) {
        BinaryOperationExecutor::execute<T, T, uint8_t, OP>(left, right, result);
    }
};

void VectorOperations::Equals(ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::Equals>(left, right, result);
}

void VectorOperations::NotEquals(ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::NotEquals>(left, right, result);
}

void VectorOperations::GreaterThan(ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::GreaterThan>(left, right, result);
}

void VectorOperations::GreaterThanEquals(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::GreaterThanEquals>(left, right, result);
}

void VectorOperations::LessThan(ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::LessThan>(left, right, result);
}

void VectorOperations::LessThanEquals(ValueVector& left, ValueVector& right, ValueVector& result) {
    ComparisonOperationExecutor::execute<operation::LessThanEquals>(left, right, result);
}

} // namespace common
} // namespace graphflow
