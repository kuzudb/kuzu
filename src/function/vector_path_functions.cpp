#include "function/path/vector_path_functions.h"

#include "binder/expression/literal_expression.h"
#include "common/string_utils.h"
#include "function/path/path_function_executor.h"
#include "function/struct/vector_struct_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_function_definitions NodesVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(NODES_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::ANY, nullptr,
        nullptr, StructExtractVectorFunctions::compileFunc, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> NodesVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = StructType::getFieldIdx(&structType, InternalKeyword::NODES);
    return std::make_unique<StructExtractBindData>(
        *(StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

vector_function_definitions RelsVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(RELS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::ANY, nullptr,
        nullptr, StructExtractVectorFunctions::compileFunc, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> RelsVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = StructType::getFieldIdx(&structType, InternalKeyword::RELS);
    return std::make_unique<StructExtractBindData>(
        *(StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

vector_function_definitions PropertiesVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(PROPERTIES_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::STRING},
        LogicalTypeID::ANY, execFunc, nullptr, compileFunc, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> PropertiesVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    if (arguments[1]->expressionType != ExpressionType::LITERAL) {
        throw BinderException(StringUtils::string_format(
            "Expect literal input as the second argument for {}().", PROPERTIES_FUNC_NAME));
    }
    auto key = ((binder::LiteralExpression&)*arguments[1]).getValue()->getValue<std::string>();
    auto listType = arguments[0]->getDataType();
    auto childType = VarListType::getChildType(&listType);
    struct_field_idx_t fieldIdx;
    if (childType->getLogicalTypeID() == LogicalTypeID::NODE ||
        childType->getLogicalTypeID() == LogicalTypeID::REL) {
        fieldIdx = StructType::getFieldIdx(childType, key);
        if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw BinderException(StringUtils::string_format("Invalid property name: {}.", key));
        }
    } else {
        throw BinderException(StringUtils::string_format(
            "Cannot extract properties from {}.", LogicalTypeUtils::dataTypeToString(listType)));
    }
    auto field = StructType::getField(childType, fieldIdx);
    auto returnType = std::make_unique<LogicalType>(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(field->getType()->copy()));
    return std::make_unique<PropertiesBindData>(*returnType, fieldIdx);
}

void PropertiesVectorFunction::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    assert(parameters[0]->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
    auto propertiesBindData = reinterpret_cast<PropertiesBindData*>(bindData);
    auto fieldVector = StructVector::getFieldVector(
        ListVector::getDataVector(parameters[0].get()), propertiesBindData->childIdx);
    ListVector::setDataVector(result.get(), fieldVector);
}

void PropertiesVectorFunction::execFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    if (parameters[0]->state->isFlat()) {
        auto inputPos = parameters[0]->state->selVector->selectedPositions[0];
        if (parameters[0]->isNull(inputPos)) {
            for (auto i = 0u; i < result.state->selVector->selectedSize; ++i) {
                auto pos = result.state->selVector->selectedPositions[i];
                result.setNull(pos, true);
            }
        } else {
            auto& listEntry = parameters[0]->getValue<list_entry_t>(inputPos);
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
                result.setValue(pos, parameters[0]->getValue<list_entry_t>(pos));
            }
        }
    }
}

vector_function_definitions IsTrailVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(IS_TRAIL_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::BOOL, execFunc,
        selectFunc, nullptr, nullptr, false /* isVarLength */));
    return definitions;
}

void IsTrailVectorFunction::execFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    UnaryPathExecutor::executeRelIDs(*parameters[0], result);
}

bool IsTrailVectorFunction::selectFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, SelectionVector& selectionVector) {
    return UnaryPathExecutor::selectRelIDs(*parameters[0], selectionVector);
}

vector_function_definitions IsACyclicVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.push_back(make_unique<VectorFunctionDefinition>(IS_ACYCLIC_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::BOOL, execFunc,
        selectFunc, nullptr, nullptr, false /* isVarLength */));
    return definitions;
}

void IsACyclicVectorFunction::execFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, ValueVector& result) {
    UnaryPathExecutor::executeNodeIDs(*parameters[0], result);
}

bool IsACyclicVectorFunction::selectFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, SelectionVector& selectionVector) {
    return UnaryPathExecutor::selectNodeIDs(*parameters[0], selectionVector);
}

} // namespace function
} // namespace kuzu
