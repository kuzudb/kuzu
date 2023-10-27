#include "function/schema/vector_node_rel_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set OffsetFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(OFFSET_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERNAL_ID}, LogicalTypeID::INT64,
        OffsetFunction::execFunction));
    return functionSet;
}

} // namespace function
} // namespace kuzu
