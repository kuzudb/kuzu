#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "common/vector/value_vector.h"
#include "function/path/vector_path_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    if (input.arguments[1]->expressionType != ExpressionType::LITERAL) {
        throw BinderException(stringFormat(
            "Expected literal input as the second argument for {}().", PropertiesFunction::name));
    }
    auto literalExpr = input.arguments[1]->constPtrCast<LiteralExpression>();
    auto key = literalExpr->getValue().getValue<std::string>();
    const auto& listType = input.arguments[0]->getDataType();
    const auto& childType = ListType::getChildType(listType);
    struct_field_idx_t fieldIdx;
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
    auto bindData = std::make_unique<PropertiesBindData>(std::move(returnType), fieldIdx);
    bindData->paramTypes.push_back(input.arguments[0]->getDataType().copy());
    bindData->paramTypes.push_back(LogicalType(input.definition->parameterTypeIDs[1]));
    return bindData;
}

static void compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters[0]->dataType.getPhysicalType() == PhysicalTypeID::LIST);
    auto& propertiesBindData = bindData->cast<PropertiesBindData>();
    auto fieldVector = StructVector::getFieldVector(ListVector::getDataVector(parameters[0].get()),
        propertiesBindData.childIdx);
    ListVector::setDataVector(result.get(), fieldVector);
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    ListVector::copyListEntryAndBufferMetaData(result, *parameters[0]);
}

function_set PropertiesFunction::getFunctionSet() {
    function_set functions;
    functions.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING}, LogicalTypeID::ANY,
        execFunc, nullptr, compileFunc, bindFunc));
    return functions;
}

} // namespace function
} // namespace kuzu
