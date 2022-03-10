#include "include/string_executor.h"
#include "include/vector_string_operations.h"
#include "operations/include/contains_operation.h"

namespace graphflow {
namespace function {

void VectorStringOperations::Contains(ValueVector& left, ValueVector& right, ValueVector& result) {
    StringOperationExecutor::execute<uint8_t, operation::Contains>(left, right, result);
}

uint64_t VectorStringOperations::ContainsSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return StringOperationExecutor::select<operation::Contains>(left, right, selectedPositions);
}

} // namespace function
} // namespace graphflow
