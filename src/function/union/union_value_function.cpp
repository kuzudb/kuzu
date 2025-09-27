#include "function/scalar_function.h"
#include "function/union/vector_union_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    KU_ASSERT(input.optionalArguments.size() == 1);
    std::vector<StructField> fields;
    if (input.optionalArguments[0]->getDataType().getLogicalTypeID() ==
        common::LogicalTypeID::ANY) {
        input.optionalArguments[0]->cast(LogicalType::STRING());
    }
    fields.emplace_back(input.optionalArguments[0]->getAlias(),
        input.optionalArguments[0]->getDataType().copy());
    auto resultType = LogicalType::UNION(std::move(fields));
    return FunctionBindData::getSimpleBindData(input.optionalArguments, resultType);
}

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>&, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    // (Tanvir) This is broken, parameters does not include optional params so we would
    // get an out of bounds error
    KU_ASSERT_UNCONDITIONAL(false);
    result.setState(parameters[0]->state);
    UnionVector::getTagVector(&result)->setState(parameters[0]->state);
    UnionVector::referenceVector(&result, UnionType::TAG_FIELD_IDX, parameters[0]);
    UnionVector::setTagField(result, *resultSelVector, UnionType::TAG_FIELD_IDX);
}

function_set UnionValueFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{},
        LogicalTypeID::UNION, execFunc);
    function->bindFunc = bindFunc;
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace function
} // namespace kuzu
