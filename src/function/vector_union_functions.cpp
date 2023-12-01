#include "function/union/vector_union_functions.h"

#include "binder/expression_binder.h"
#include "function/struct/vector_struct_functions.h"
#include "function/union/functions/union_tag.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set UnionValueFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(UNION_VALUE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::UNION, execFunc, nullptr,
        compileFunc, bindFunc, false /* isVarLength */));
    return functionSet;
}

std::unique_ptr<FunctionBindData> UnionValueFunction::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::Function* /*function*/) {
    KU_ASSERT(arguments.size() == 1);
    std::vector<std::unique_ptr<StructField>> fields;
    // TODO(Ziy): Use UINT8 to represent tag value.
    fields.push_back(std::make_unique<StructField>(
        UnionType::TAG_FIELD_NAME, std::make_unique<LogicalType>(UnionType::TAG_FIELD_TYPE)));
    if (arguments[0]->getDataType().getLogicalTypeID() == common::LogicalTypeID::ANY) {
        binder::ExpressionBinder::resolveAnyDataType(
            *arguments[0], LogicalType(LogicalTypeID::STRING));
    }
    fields.push_back(std::make_unique<StructField>(
        arguments[0]->getAlias(), arguments[0]->getDataType().copy()));
    auto resultType =
        LogicalType(LogicalTypeID::UNION, std::make_unique<StructTypeInfo>(std::move(fields)));
    return std::make_unique<FunctionBindData>(resultType);
}

void UnionValueFunction::execFunc(const std::vector<std::shared_ptr<ValueVector>>& /*parameters*/,
    ValueVector& result, void* /*dataPtr*/) {
    UnionVector::setTagField(&result, UnionType::TAG_FIELD_IDX);
}

void UnionValueFunction::compileFunc(FunctionBindData* /*bindData*/,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters.size() == 1);
    result->setState(parameters[0]->state);
    UnionVector::getTagVector(result.get())->setState(parameters[0]->state);
    UnionVector::referenceVector(result.get(), UnionType::TAG_FIELD_IDX, parameters[0]);
}

function_set UnionTagFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(UNION_TAG_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::UNION}, LogicalTypeID::STRING,
        ScalarFunction::UnaryExecListStructFunction<union_entry_t, ku_string_t, UnionTag>, nullptr,
        nullptr, false /* isVarLength */));
    return functionSet;
}

function_set UnionExtractFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(UNION_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::UNION, LogicalTypeID::STRING}, LogicalTypeID::ANY,
        nullptr, nullptr, StructExtractFunctions::compileFunc, StructExtractFunctions::bindFunc,
        false /* isVarLength */));
    return functionSet;
}

} // namespace function
} // namespace kuzu
