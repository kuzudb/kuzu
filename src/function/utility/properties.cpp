#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "common/vector/value_vector.h"
#include "function/scalar_function.h"
#include "function/utility/vector_utility_functions.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindStructFunc(const ScalarBindFuncInput& input) {
    std::vector<StructField> fields;
    const auto& structType = input.arguments[0]->getDataType();
    auto keys = StructType::getFieldNames(structType);
    std::vector<common::idx_t> fieldIdxs;
    for (auto& key : keys) {
        if (key == InternalKeyword::ID || key == InternalKeyword::LABEL ||
            key == InternalKeyword::SRC || key == InternalKeyword::DST) {
            continue;
        }
        auto fieldIdx = StructType::getFieldIdx(structType, key);
        auto fieldType = StructType::getField(structType, fieldIdx).getType().copy();
        if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw BinderException(stringFormat("Invalid struct field name: {}.", key));
        }
        fieldIdxs.push_back(fieldIdx);
        fields.emplace_back(key, std::move(fieldType));
    }
    const auto resultType = LogicalType::STRUCT(std::move(fields));
    auto bindData = std::make_unique<StructPropertiesBindData>(resultType.copy(), fieldIdxs);
    bindData->paramTypes.push_back(input.arguments[0]->getDataType().copy());
    return bindData;
}

static void compileStructFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    auto& propertiesBindData = bindData->cast<StructPropertiesBindData>();
    std::vector<std::shared_ptr<ValueVector>> fieldVectors;
    for (auto fieldIdx : propertiesBindData.childIdxs) {
        auto fieldVector = StructVector::getFieldVector(parameters[0].get(), fieldIdx);
        fieldVectors.push_back(fieldVector);
    }
    for (auto i = 0u; i < fieldVectors.size(); ++i) {
        StructVector::referenceVector(result.get(), i, fieldVectors[i]);
    }
}

static std::unique_ptr<FunctionBindData> bindPathFunc(const ScalarBindFuncInput& input) {
    if (input.arguments[1]->expressionType != ExpressionType::LITERAL) {
        throw BinderException(stringFormat(
            "Expected literal input as the second argument for {}().", PropertiesFunctions::name));
    }
    auto literalExpr = input.arguments[1]->constPtrCast<LiteralExpression>();
    auto key = literalExpr->getValue().getValue<std::string>();
    const auto& listType = input.arguments[0]->getDataType();
    const auto& childType = ListType::getChildType(listType);
    struct_field_idx_t fieldIdx = 0;
    if (childType.getLogicalTypeID() == LogicalTypeID::NODE ||
        childType.getLogicalTypeID() == LogicalTypeID::REL) {
        fieldIdx = StructType::getFieldIdx(childType, key);
        if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw BinderException(stringFormat("Invalid property name: {}.", key));
        }
    } else {
        throw BinderException(
            stringFormat("Cannot extract properties from {}.", listType.toString()));
    }
    const auto& field = StructType::getField(childType, fieldIdx);
    auto returnType = LogicalType::LIST(field.getType().copy());
    auto bindData = std::make_unique<PathPropertiesBindData>(std::move(returnType), fieldIdx);
    bindData->paramTypes.push_back(input.arguments[0]->getDataType().copy());
    bindData->paramTypes.push_back(LogicalType(input.definition->parameterTypeIDs[1]));
    return bindData;
}

static void compilePathFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters[0]->dataType.getPhysicalType() == PhysicalTypeID::LIST);
    auto& propertiesBindData = bindData->cast<PathPropertiesBindData>();
    auto fieldVector = StructVector::getFieldVector(ListVector::getDataVector(parameters[0].get()),
        propertiesBindData.childIdx);
    ListVector::setDataVector(result.get(), fieldVector);
}

static void execPathFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    ListVector::copyListEntryAndBufferMetaData(result, *resultSelVector, *parameters[0],
        *parameterSelVectors[0]);
}

static std::unique_ptr<Function> getStructPropertiesFunction(LogicalTypeID logicalTypeID) {
    auto function = std::make_unique<ScalarFunction>(PropertiesFunctions::name,
        std::vector<LogicalTypeID>{logicalTypeID}, LogicalTypeID::STRUCT);
    function->bindFunc = bindStructFunc;
    function->compileFunc = compileStructFunc;
    return function;
}

function_set PropertiesFunctions::getFunctionSet() {
    function_set functions;
    auto inputTypeIDs = // PROPERTIES(STRUCT)
        std::vector<LogicalTypeID>{LogicalTypeID::STRUCT, LogicalTypeID::NODE, LogicalTypeID::REL};
    for (auto inputTypeID : inputTypeIDs) {
        functions.push_back(getStructPropertiesFunction(inputTypeID));
    }
    auto function = std::make_unique<ScalarFunction>(name, // PROPERTIES(PATH)
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING}, LogicalTypeID::ANY,
        execPathFunc);
    function->bindFunc = bindPathFunc;
    function->compileFunc = compilePathFunc;
    functions.push_back(std::move(function));
    return functions;
}

} // namespace function
} // namespace kuzu
