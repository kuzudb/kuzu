#include "include/string_executor.h"
#include "include/vector_string_operations.h"
#include "operations/include/starts_with_operation.h"

namespace graphflow {
namespace function {

void VectorStringOperations::StartsWith(
    ValueVector& left, ValueVector& right, ValueVector& result) {
    StringOperationExecutor::execute<uint8_t, operation::StartsWith>(left, right, result);
}

uint64_t VectorStringOperations::StartsWithSelect(
    ValueVector& left, ValueVector& right, sel_t* selectedPositions) {
    return StringOperationExecutor::select<operation::StartsWith>(left, right, selectedPositions);
}

} // namespace function
} // namespace graphflow
