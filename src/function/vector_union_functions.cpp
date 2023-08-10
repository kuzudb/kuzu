#include "function/union/vector_union_functions.h"

#include "function/struct/vector_struct_functions.h"
#include "function/union/functions/union_tag.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_function_definitions UnionValueVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(UNION_VALUE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::UNION, execFunc, nullptr,
        compileFunc, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> UnionValueVectorFunction::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    assert(arguments.size() == 1);
    std::vector<std::unique_ptr<StructField>> fields;
    // TODO(Ziy): Use UINT8 to represent tag value.
    fields.push_back(std::make_unique<StructField>(
        UnionType::TAG_FIELD_NAME, std::make_unique<LogicalType>(UnionType::TAG_FIELD_TYPE)));
    fields.push_back(std::make_unique<StructField>(
        arguments[0]->getAlias(), std::make_unique<LogicalType>(arguments[0]->getDataType())));
    auto resultType =
        LogicalType(LogicalTypeID::UNION, std::make_unique<StructTypeInfo>(std::move(fields)));
    return std::make_unique<FunctionBindData>(resultType);
}

void UnionValueVectorFunction::execFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    UnionVector::setTagField(&result, UnionType::TAG_FIELD_IDX);
}

void UnionValueVectorFunction::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    assert(parameters.size() == 1);
    result->setState(parameters[0]->state);
    UnionVector::getTagVector(result.get())->setState(parameters[0]->state);
    UnionVector::referenceVector(result.get(), UnionType::TAG_FIELD_IDX, parameters[0]);
}

vector_function_definitions UnionTagVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(UNION_TAG_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::UNION}, LogicalTypeID::STRING,
        UnaryExecListStructFunction<struct_entry_t, ku_string_t, UnionTag>, nullptr, nullptr,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions UnionExtractVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(UNION_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::UNION, LogicalTypeID::STRING}, LogicalTypeID::ANY,
        nullptr, nullptr, StructExtractVectorFunctions::compileFunc,
        StructExtractVectorFunctions::bindFunc, false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace kuzu
