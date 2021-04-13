#include "src/common/include/vector/operations/vector_boolean_operations.h"

#include "src/common/include/operations/boolean_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"

namespace graphflow {
namespace common {

struct UnaryBooleanOperationExecutor {
public:
    template<class OP>
    static inline void execute(ValueVector& operand, ValueVector& result) {
        switch (operand.dataType) {
        case BOOL:
            operate<uint8_t, OP>(operand, result);
            break;
        case INT32:
            operate<int32_t, OP>(operand, result);
            break;
        case DOUBLE:
            operate<double_t, OP>(operand, result);
            break;
        /* TODO: add gf_string_t comparison. */
        default:
            throw std::invalid_argument("Invalid or unsupported type for comparison.");
        }
    }

private:
    template<class T, class OP>
    static inline void operate(ValueVector& operand, ValueVector& result) {
        UnaryOperationExecutor::executeNonBoolOp<T, uint8_t, OP>(operand, result);
    }
};

void VectorBooleanOperations::And(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeBoolOp<operation::And>(left, right, result);
}

void VectorBooleanOperations::Or(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeBoolOp<operation::Or>(left, right, result);
}

void VectorBooleanOperations::Xor(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::executeBoolOp<operation::Xor>(left, right, result);
}

void VectorBooleanOperations::Not(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::executeBoolOp<operation::Not>(operand, result);
}

} // namespace common
} // namespace graphflow
