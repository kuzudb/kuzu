#include "src/common/include/operations/boolean_operations.h"
#include "src/common/include/vector/operations/executors/binary_operation_executor.h"
#include "src/common/include/vector/operations/executors/unary_operation_executor.h"
#include "src/common/include/vector/operations/vector_operations.h"

namespace graphflow {
namespace common {

struct UnaryBooleanOperationExecutor {
public:
    template<class OP>
    static inline void execute(ValueVector& operand, ValueVector& result) {
        switch (operand.getDataType()) {
        case BOOL:
            operate<uint8_t, OP>(operand, result);
            break;
        case INT:
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
        UnaryOperationExecutor::execute<T, uint8_t, OP>(operand, result);
    }
};

void VectorOperations::And(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::execute<uint8_t, uint8_t, uint8_t, operation::And>(
        left, right, result);
}

void VectorOperations::Or(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::execute<uint8_t, uint8_t, uint8_t, operation::Or>(left, right, result);
}

void VectorOperations::Xor(ValueVector& left, ValueVector& right, ValueVector& result) {
    BinaryOperationExecutor::execute<uint8_t, uint8_t, uint8_t, operation::Xor>(
        left, right, result);
}

void VectorOperations::Not(ValueVector& operand, ValueVector& result) {
    UnaryOperationExecutor::execute<uint8_t, uint8_t, operation::Not>(operand, result);
}

} // namespace common
} // namespace graphflow
