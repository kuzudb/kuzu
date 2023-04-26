#include "function/struct/vector_struct_operations.h"

#include "binder/expression/literal_expression.h"
#include "function/function_definition.h"

namespace kuzu {
namespace function {

std::vector<std::unique_ptr<VectorOperationDefinition>>
StructPackVectorOperations::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::STRUCT_PACK_FUNC_NAME,
        std::vector<common::DataTypeID>{common::ANY}, common::STRUCT, execFunc, nullptr, bindFunc,
        true /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> StructPackVectorOperations::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    std::vector<std::unique_ptr<common::StructField>> fields;
    for (auto& argument : arguments) {
        fields.emplace_back(std::make_unique<common::StructField>(
            argument->getAlias(), argument->getDataType().copy()));
    }
    auto resultType = common::DataType(std::move(fields));
    return std::make_unique<FunctionBindData>(resultType);
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
StructExtractVectorOperations::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::STRUCT_EXTRACT_FUNC_NAME,
        std::vector<common::DataTypeID>{common::STRUCT, common::STRING}, common::ANY, execFunc,
        nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> StructExtractVectorOperations::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    auto typeInfo = reinterpret_cast<common::StructTypeInfo*>(structType.getExtraTypeInfo());
    if (arguments[1]->expressionType != common::LITERAL) {
        throw common::BinderException("Key name for struct_extract must be STRING literal.");
    }
    auto key = ((binder::LiteralExpression&)*arguments[1]).getValue()->getValue<std::string>();
    common::StringUtils::toUpper(key);
    assert(definition->returnTypeID == common::ANY);
    auto childrenTypes = typeInfo->getChildrenTypes();
    auto childrenNames = typeInfo->getChildrenNames();
    for (auto i = 0u; i < childrenNames.size(); ++i) {
        if (childrenNames[i] == key) {
            return std::make_unique<StructExtractBindData>(*childrenTypes[i], i);
        }
    }
    throw common::BinderException("Cannot find key " + key + " for struct_extract.");
}

} // namespace function
} // namespace kuzu
