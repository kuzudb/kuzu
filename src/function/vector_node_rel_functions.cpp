#include "function/schema/vector_node_rel_functions.h"

namespace kuzu {
namespace function {

vector_function_definitions OffsetVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::OFFSET_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::INTERNAL_ID},
        common::LogicalTypeID::INT64, OffsetVectorFunction::execFunction));
    return definitions;
}

} // namespace function
} // namespace kuzu
