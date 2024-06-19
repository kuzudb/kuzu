#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "function/scalar_function.h"
#include "function/struct/vector_struct_functions.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

std::unique_ptr<FunctionBindData> StructExtractFunctions::bindFunc(
    const expression_vector& arguments, Function* function) {
    const auto& structType = arguments[0]->getDataType();
    if (arguments[1]->expressionType != ExpressionType::LITERAL) {
        throw BinderException("Key name for struct/union extract must be STRING literal.");
    }
    auto key = arguments[1]->constPtrCast<LiteralExpression>()->getValue().getValue<std::string>();
    auto fieldIdx = StructType::getFieldIdx(structType, key);
    if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
        throw BinderException(stringFormat("Invalid struct field name: {}.", key));
    }
    auto paramTypes = ExpressionUtil::getDataTypes(arguments);
    auto resultType = StructType::getField(structType, fieldIdx).getType().copy();
    auto bindData = std::make_unique<StructExtractBindData>(std::move(resultType), fieldIdx);
    bindData->paramTypes.push_back(arguments[0]->getDataType().copy());
    bindData->paramTypes.push_back(LogicalType(function->parameterTypeIDs[1]));
    return bindData;
}

void StructExtractFunctions::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters[0]->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structBindData = ku_dynamic_cast<FunctionBindData*, StructExtractBindData*>(bindData);
    result = StructVector::getFieldVector(parameters[0].get(), structBindData->childIdx);
    result->state = parameters[0]->state;
}

static std::unique_ptr<ScalarFunction> getStructExtractFunction(LogicalTypeID logicalTypeID) {
    return std::make_unique<ScalarFunction>(StructExtractFunctions::name,
        std::vector<LogicalTypeID>{logicalTypeID, LogicalTypeID::STRING}, LogicalTypeID::ANY,
        nullptr, nullptr, StructExtractFunctions::compileFunc, StructExtractFunctions::bindFunc);
}

function_set StructExtractFunctions::getFunctionSet() {
    function_set functions;
    auto inputTypeIDs =
        std::vector<LogicalTypeID>{LogicalTypeID::STRUCT, LogicalTypeID::NODE, LogicalTypeID::REL};
    for (auto inputTypeID : inputTypeIDs) {
        functions.push_back(getStructExtractFunction(inputTypeID));
    }
    return functions;
}

} // namespace function
} // namespace kuzu
