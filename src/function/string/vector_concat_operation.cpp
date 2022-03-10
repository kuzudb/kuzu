#include "include/string_executor.h"
#include "include/vector_string_operations.h"
#include "operations/include/concat_operation.h"

namespace graphflow {
namespace function {

void VectorStringOperations::Concat(ValueVector& left, ValueVector& right, ValueVector& result) {
    StringOperationExecutor::execute<gf_string_t, operation::Concat>(left, right, result);
}

} // namespace function
} // namespace graphflow
