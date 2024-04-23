#include "function/scalar_function.h"
#include "function/union/vector_union_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
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
    return FunctionBindData::getSimpleBindData(arguments, *resultType);
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& /*parameters*/,
    ValueVector& result, void* /*dataPtr*/) {
    UnionVector::setTagField(&result, UnionType::TAG_FIELD_IDX);
}

static void valueCompileFunc(FunctionBindData* /*bindData*/,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters.size() == 1);
    result->setState(parameters[0]->state);
    UnionVector::getTagVector(result.get())->setState(parameters[0]->state);
    UnionVector::referenceVector(result.get(), UnionType::TAG_FIELD_IDX, parameters[0]);
}

function_set UnionValueFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
            LogicalTypeID::UNION, execFunc, nullptr, valueCompileFunc, bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
