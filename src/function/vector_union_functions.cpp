#include "function/union/vector_union_functions.h"

#include "function/scalar_function.h"
#include "function/struct/vector_struct_functions.h"
#include "function/union/functions/union_tag.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> UnionValueBindFunc(
    const binder::expression_vector& arguments, kuzu::function::Function* /*function*/) {
    KU_ASSERT(arguments.size() == 1);
    std::vector<StructField> fields;
    // TODO(Ziy): Use UINT8 to represent tag value.
    fields.emplace_back(UnionType::TAG_FIELD_NAME,
        std::make_unique<LogicalType>(UnionType::TAG_FIELD_TYPE));
    if (arguments[0]->getDataType().getLogicalTypeID() == common::LogicalTypeID::ANY) {
        arguments[0]->cast(*LogicalType::STRING());
    }
    fields.emplace_back(arguments[0]->getAlias(), arguments[0]->getDataType().copy());
    auto resultType = LogicalType::UNION(std::move(fields));
    return std::make_unique<FunctionBindData>(std::move(resultType));
}

static void UnionValueExecFunc(const std::vector<std::shared_ptr<ValueVector>>& /*parameters*/,
    ValueVector& result, void* /*dataPtr*/) {
    UnionVector::setTagField(&result, UnionType::TAG_FIELD_IDX);
}

static void UnionValueCompileFunc(FunctionBindData* /*bindData*/,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters.size() == 1);
    result->setState(parameters[0]->state);
    UnionVector::getTagVector(result.get())->setState(parameters[0]->state);
    UnionVector::referenceVector(result.get(), UnionType::TAG_FIELD_IDX, parameters[0]);
}

function_set UnionValueFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::UNION, UnionValueExecFunc,
        nullptr, UnionValueCompileFunc, UnionValueBindFunc, false /* isVarLength */));
    return functionSet;
}

function_set UnionTagFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::UNION}, LogicalTypeID::STRING,
        ScalarFunction::UnaryExecNestedTypeFunction<union_entry_t, ku_string_t, UnionTag>, nullptr,
        nullptr, false /* isVarLength */));
    return functionSet;
}

function_set UnionExtractFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::UNION, LogicalTypeID::STRING}, LogicalTypeID::ANY,
        nullptr, nullptr, StructExtractFunctions::compileFunc, StructExtractFunctions::bindFunc,
        false /* isVarLength */));
    return functionSet;
}

} // namespace function
} // namespace kuzu
