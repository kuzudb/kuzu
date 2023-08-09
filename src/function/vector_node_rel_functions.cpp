#include "function/schema/vector_node_rel_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_function_definitions OffsetVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(OFFSET_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERNAL_ID}, LogicalTypeID::INT64,
        OffsetVectorFunction::execFunction));
    return definitions;
}

} // namespace function
} // namespace kuzu
