#include "function/union/vector_union_operations.h"

#include "function/struct/vector_struct_operations.h"
#include "function/union/operations/union_tag.h"

namespace kuzu {
namespace function {

vector_operation_definitions UnionValueVectorOperations::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::UNION_VALUE_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::ANY},
        common::LogicalTypeID::UNION, execFunc, nullptr, compileFunc, bindFunc,
        false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> UnionValueVectorOperations::bindFunc(
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

void UnionValueVectorOperations::execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    common::ValueVector& result) {
    common::UnionVector::setTagField(&result, 0 /* tagFieldIdx */);
}

void UnionValueVectorOperations::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    std::shared_ptr<common::ValueVector>& result) {
    assert(parameters.size() == 1);
    result->setState(parameters[0]->state);
    common::UnionVector::getTagVector(result.get())->setState(parameters[0]->state);
    common::UnionVector::referenceVector(result.get(), 0 /* fieldIdx */, parameters[0]);
}

vector_operation_definitions UnionTagVectorOperations::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::UNION_TAG_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::UNION},
        common::LogicalTypeID::STRING,
        UnaryExecListStructFunction<common::struct_entry_t, common::ku_string_t,
            operation::UnionTag>,
        nullptr, nullptr, false /* isVarLength */));
    return definitions;
}

vector_operation_definitions UnionExtractVectorOperations::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::UNION_EXTRACT_FUNC_NAME,
        std::vector<common::LogicalTypeID>{
            common::LogicalTypeID::UNION, common::LogicalTypeID::STRING},
        common::LogicalTypeID::ANY, nullptr, nullptr, StructExtractVectorOperations::compileFunc,
        StructExtractVectorOperations::bindFunc, false /* isVarLength */));
    return definitions;
}

} // namespace function
} // namespace kuzu
