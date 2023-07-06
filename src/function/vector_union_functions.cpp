#include "function/union/vector_union_functions.h"

#include "function/struct/vector_struct_functions.h"
#include "function/union/functions/union_tag.h"

namespace kuzu {
namespace function {

vector_function_definitions UnionValueVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::UNION_VALUE_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::ANY},
        common::LogicalTypeID::UNION, execFunc, nullptr, compileFunc, bindFunc,
        false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> UnionValueVectorFunction::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    assert(arguments.size() == 1);
    std::vector<std::unique_ptr<common::StructField>> fields;
    // TODO(Ziy): Use UINT8 to represent tag value.
    fields.push_back(std::make_unique<common::StructField>(common::InternalKeyword::TAG,
        std::make_unique<common::LogicalType>(common::LogicalTypeID::INT64)));
    fields.push_back(std::make_unique<common::StructField>(arguments[0]->getAlias(),
        std::make_unique<common::LogicalType>(arguments[0]->getDataType())));
    auto resultType = common::LogicalType(
        common::LogicalTypeID::UNION, std::make_unique<common::StructTypeInfo>(std::move(fields)));
    return std::make_unique<FunctionBindData>(resultType);
}

void UnionValueVectorFunction::execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    common::ValueVector& result) {
    common::UnionVector::setTagField(&result, 0 /* tagFieldIdx */);
}

void UnionValueVectorFunction::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    std::shared_ptr<common::ValueVector>& result) {
    assert(parameters.size() == 1);
    result->setState(parameters[0]->state);
    common::UnionVector::getTagVector(result.get())->setState(parameters[0]->state);
    common::UnionVector::referenceVector(result.get(), 0 /* fieldIdx */, parameters[0]);
}

vector_function_definitions UnionTagVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::UNION_TAG_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::UNION},
        common::LogicalTypeID::STRING,
        UnaryExecListStructFunction<common::struct_entry_t, common::ku_string_t, UnionTag>, nullptr,
        nullptr, false /* isVarLength */));
    return definitions;
}

vector_function_definitions UnionExtractVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::UNION_EXTRACT_FUNC_NAME,
        std::vector<common::LogicalTypeID>{
            common::LogicalTypeID::UNION, common::LogicalTypeID::STRING},
        common::LogicalTypeID::ANY, nullptr, nullptr, StructExtractVectorFunctions::compileFunc,
        StructExtractVectorFunctions::bindFunc, false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace kuzu
