#include "function/struct/vector_struct_operations.h"

#include "binder/expression/literal_expression.h"
#include "binder/expression_binder.h"
#include "function/function_definition.h"

namespace kuzu {
namespace function {

std::vector<std::unique_ptr<VectorOperationDefinition>>
StructPackVectorOperations::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::STRUCT_PACK_FUNC_NAME,
        std::vector<common::DataTypeID>{common::ANY}, common::STRUCT, execFunc, nullptr, bindFunc,
        true /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> StructPackVectorOperations::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    std::vector<std::unique_ptr<common::StructField>> fields;
    for (auto& argument : arguments) {
        if (argument->getDataType().typeID == common::ANY) {
            binder::ExpressionBinder::resolveAnyDataType(
                *argument, common::DataType{common::INT64});
        }
        fields.emplace_back(std::make_unique<common::StructField>(
            argument->getAlias(), argument->getDataType().copy()));
    }
    auto resultType = common::DataType(std::move(fields));
    return std::make_unique<FunctionBindData>(resultType);
}

void StructPackVectorOperations::execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    common::ValueVector& result) {
    for (auto i = 0u; i < parameters.size(); i++) {
        auto& parameter = parameters[i];
        if (parameter->state == result.state) {
            continue;
        }
        // If the parameter's state is inconsistent with the result's state, we need to copy the
        // parameter's value to the corresponding child vector.
        copyParameterValueToStructFieldVector(
            parameter.get(), common::StructVector::getChildVector(&result, i).get());
    }
}

void StructPackVectorOperations::copyParameterValueToStructFieldVector(
    const common::ValueVector* parameter, common::ValueVector* structField) {
    // If the parameter is unFlat, then its state must be consistent with the result's state.
    // Thus, we don't need to copy values to structFieldVector.
    assert(parameter->state->isFlat());
    auto srcPos = parameter->state->selVector->selectedPositions[0];
    auto srcValue = parameter->getData() + parameter->getNumBytesPerValue() * srcPos;
    bool isSrcValueNull = parameter->isNull(srcPos);
    if (structField->state->isFlat()) {
        auto pos = structField->state->selVector->selectedPositions[0];
        if (isSrcValueNull) {
            structField->setNull(pos, true /* isNull */);
        } else {
            common::ValueVectorUtils::copyValue(
                structField->getData() + structField->getNumBytesPerValue() * pos, *structField,
                srcValue, *parameter);
        }
    } else {
        for (auto j = 0u; j < structField->state->selVector->selectedSize; j++) {
            auto pos = structField->state->selVector->selectedPositions[j];
            if (isSrcValueNull) {
                structField->setNull(pos, true /* isNull */);
            } else {
                common::ValueVectorUtils::copyValue(
                    structField->getData() + structField->getNumBytesPerValue() * pos, *structField,
                    srcValue, *parameter);
            }
        }
    }
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
StructExtractVectorOperations::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(common::STRUCT_EXTRACT_FUNC_NAME,
        std::vector<common::DataTypeID>{common::STRUCT, common::STRING}, common::ANY, execFunc,
        nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> StructExtractVectorOperations::bindFunc(
    const binder::expression_vector& arguments, kuzu::function::FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    auto typeInfo = reinterpret_cast<common::StructTypeInfo*>(structType.getExtraTypeInfo());
    if (arguments[1]->expressionType != common::LITERAL) {
        throw common::BinderException("Key name for struct_extract must be STRING literal.");
    }
    auto key = ((binder::LiteralExpression&)*arguments[1]).getValue()->getValue<std::string>();
    common::StringUtils::toUpper(key);
    assert(definition->returnTypeID == common::ANY);
    auto childrenTypes = typeInfo->getChildrenTypes();
    auto childrenNames = typeInfo->getChildrenNames();
    for (auto i = 0u; i < childrenNames.size(); ++i) {
        if (childrenNames[i] == key) {
            return std::make_unique<StructExtractBindData>(*childrenTypes[i], i);
        }
    }
    throw common::BinderException("Cannot find key " + key + " for struct_extract.");
}

} // namespace function
} // namespace kuzu
