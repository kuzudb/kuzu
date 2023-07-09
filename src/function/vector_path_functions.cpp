#include "function/path/vector_path_functions.h"

#include "binder/expression/literal_expression.h"
#include "function/struct/vector_struct_functions.h"

namespace kuzu {
namespace function {

vector_function_definitions NodesVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::NODES_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::RECURSIVE_REL},
        common::LogicalTypeID::ANY, nullptr, nullptr, StructExtractVectorFunctions::compileFunc,
        bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> NodesVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = common::StructType::getFieldIdx(&structType, common::InternalKeyword::NODES);
    return std::make_unique<StructExtractBindData>(
        *(common::StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

vector_function_definitions RelsVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::RELS_FUNC_NAME,
        std::vector<common::LogicalTypeID>{common::LogicalTypeID::RECURSIVE_REL},
        common::LogicalTypeID::ANY, nullptr, nullptr, StructExtractVectorFunctions::compileFunc,
        bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> RelsVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = common::StructType::getFieldIdx(&structType, common::InternalKeyword::RELS);
    return std::make_unique<StructExtractBindData>(
        *(common::StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

vector_function_definitions PropertiesVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(common::PROPERTIES_FUNC_NAME,
        std::vector<common::LogicalTypeID>{
            common::LogicalTypeID::VAR_LIST, common::LogicalTypeID::STRING},
        common::LogicalTypeID::ANY, execFunc, nullptr, compileFunc, bindFunc,
        false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> PropertiesVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    if (arguments[1]->expressionType != common::ExpressionType::LITERAL) {
        throw common::BinderException(common::StringUtils::string_format(
            "Expect literal input as the second argument for {}().", common::PROPERTIES_FUNC_NAME));
    }
    auto key = ((binder::LiteralExpression&)*arguments[1]).getValue()->getValue<std::string>();
    auto listType = arguments[0]->getDataType();
    auto childType = common::VarListType::getChildType(&listType);
    common::struct_field_idx_t fieldIdx;
    if (childType->getLogicalTypeID() == common::LogicalTypeID::NODE ||
        childType->getLogicalTypeID() == common::LogicalTypeID::REL) {
        fieldIdx = common::StructType::getFieldIdx(childType, key);
        if (fieldIdx == common::INVALID_STRUCT_FIELD_IDX) {
            throw common::BinderException(
                common::StringUtils::string_format("Invalid property name: {}.", key));
        }
    } else {
        throw common::BinderException(
            common::StringUtils::string_format("Cannot extract properties from {}.",
                common::LogicalTypeUtils::dataTypeToString(listType)));
    }
    auto field = common::StructType::getField(childType, fieldIdx);
    auto returnType = std::make_unique<common::LogicalType>(common::LogicalTypeID::VAR_LIST,
        std::make_unique<common::VarListTypeInfo>(field->getType()->copy()));
    return std::make_unique<PropertiesBindData>(*returnType, fieldIdx);
}

void PropertiesVectorFunction::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    std::shared_ptr<common::ValueVector>& result) {
    assert(parameters[0]->dataType.getPhysicalType() == common::PhysicalTypeID::VAR_LIST);
    auto propertiesBindData = reinterpret_cast<PropertiesBindData*>(bindData);
    auto fieldVector = common::StructVector::getFieldVector(
        common::ListVector::getDataVector(parameters[0].get()), propertiesBindData->childIdx);
    common::ListVector::setDataVector(result.get(), fieldVector);
}

void PropertiesVectorFunction::execFunc(
    const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    common::ValueVector& result) {
    if (parameters[0]->state->isFlat()) {
        auto inputPos = parameters[0]->state->selVector->selectedPositions[0];
        if (parameters[0]->isNull(inputPos)) {
            for (auto i = 0u; i < result.state->selVector->selectedSize; ++i) {
                auto pos = result.state->selVector->selectedPositions[i];
                result.setNull(pos, true);
            }
        } else {
            auto& listEntry = parameters[0]->getValue<common::list_entry_t>(inputPos);
            for (auto i = 0u; i < result.state->selVector->selectedSize; ++i) {
                auto pos = result.state->selVector->selectedPositions[i];
                result.setValue(pos, listEntry);
            }
        }
    } else {
        for (auto i = 0u; i < result.state->selVector->selectedSize; ++i) {
            auto pos = result.state->selVector->selectedPositions[i];
            if (parameters[0]->isNull(pos)) {
                result.setNull(pos, true);
            } else {
                result.setValue(pos, parameters[0]->getValue<common::list_entry_t>(pos));
            }
        }
    }
}

} // namespace function
} // namespace kuzu
