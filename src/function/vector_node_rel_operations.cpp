#include "function/schema/vector_node_rel_operations.h"

#include "function/struct/vector_struct_operations.h"

namespace kuzu {
namespace function {

vector_operation_definitions OffsetVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::OFFSET_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::INTERNAL_ID},
        common::LogicalTypeID::INT64, OffsetVectorOperation::execFunction));
    return definitions;
}

vector_operation_definitions NodesVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::NODES_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::RECURSIVE_REL},
        common::LogicalTypeID::ANY, nullptr, nullptr, StructExtractVectorOperations::compileFunc,
        bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> NodesVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = common::StructType::getFieldIdx(&structType, common::InternalKeyword::NODES);
    return std::make_unique<StructExtractBindData>(
        *(common::StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

vector_operation_definitions RelsVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::RELS_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::RECURSIVE_REL},
        common::LogicalTypeID::ANY, nullptr, nullptr, StructExtractVectorOperations::compileFunc,
        bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> RelsVectorOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = common::StructType::getFieldIdx(&structType, common::InternalKeyword::RELS);
    return std::make_unique<StructExtractBindData>(
        *(common::StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

} // namespace function
} // namespace kuzu
