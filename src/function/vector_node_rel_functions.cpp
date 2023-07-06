#include "function/schema/vector_node_rel_functions.h"

#include "function/struct/vector_struct_functions.h"

namespace kuzu {
namespace function {

vector_function_definitions OffsetVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::OFFSET_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::INTERNAL_ID},
        common::LogicalTypeID::INT64, OffsetVectorFunction::execFunction));
    return definitions;
}

vector_function_definitions NodesVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::NODES_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::RECURSIVE_REL},
        common::LogicalTypeID::ANY, nullptr, nullptr, StructExtractVectorFunctions::compileFunc,
        bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> NodesVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = common::StructType::getFieldIdx(&structType, common::InternalKeyword::NODES);
    return std::make_unique<StructExtractBindData>(
        *(common::StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

vector_function_definitions RelsVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::RELS_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::RECURSIVE_REL},
        common::LogicalTypeID::ANY, nullptr, nullptr, StructExtractVectorFunctions::compileFunc,
        bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> RelsVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = common::StructType::getFieldIdx(&structType, common::InternalKeyword::RELS);
    return std::make_unique<StructExtractBindData>(
        *(common::StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

} // namespace function
} // namespace kuzu
