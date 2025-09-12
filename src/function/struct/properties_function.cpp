#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "binder/expression/scalar_function_expression.h"
#include "binder/expression_binder.h"
#include "common/exception/binder.h"
#include "function/rewrite_function.h"
#include "function/struct/vector_struct_functions.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::catalog;

namespace kuzu {
namespace function {

std::unique_ptr<FunctionBindData> StructPropertiesFunctions::bindFunc(
    const ScalarBindFuncInput& input) {
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

void StructPropertiesFunctions::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters[0]->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
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

static std::unique_ptr<Function> getPropertiesFunction(LogicalTypeID logicalTypeID) {
    auto function = std::make_unique<ScalarFunction>(StructPropertiesFunctions::name,
        std::vector<LogicalTypeID>{logicalTypeID}, LogicalTypeID::STRUCT);
    function->bindFunc = StructPropertiesFunctions::bindFunc;
    function->compileFunc = StructPropertiesFunctions::compileFunc;
    return function;
}

function_set StructPropertiesFunctions::getFunctionSet() {
    function_set functions;
    auto inputTypeIDs =
        std::vector<LogicalTypeID>{LogicalTypeID::STRUCT, LogicalTypeID::NODE, LogicalTypeID::REL};
    for (auto inputTypeID : inputTypeIDs) {
        functions.push_back(getPropertiesFunction(inputTypeID));
    }
    return functions;
}

} // namespace function
} // namespace kuzu
