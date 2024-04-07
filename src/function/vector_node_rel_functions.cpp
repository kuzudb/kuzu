#include "function/schema/vector_node_rel_functions.h"

#include "common/vector/value_vector.h"
#include "function/scalar_function.h"
#include "function/schema/offset_functions.h"
#include "function/unary_function_executor.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static void OffsetExecFunc(const std::vector<std::shared_ptr<ValueVector>>& params,
    ValueVector& result, void* /*dataPtr*/ = nullptr) {
    KU_ASSERT(params.size() == 1);
    UnaryFunctionExecutor::execute<internalID_t, int64_t, Offset>(*params[0], result);
}

function_set OffsetFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::INTERNAL_ID},
            LogicalTypeID::INT64, OffsetExecFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
