#include "function/path/vector_path_functions.h"

#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "function/path/path_function_executor.h"
#include "function/struct/vector_struct_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set NodesFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(NODES_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::ANY, nullptr,
        nullptr, StructExtractFunctions::compileFunc, bindFunc, false /* isVarLength */));
    return functionSet;
}

std::unique_ptr<FunctionBindData> NodesFunction::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = StructType::getFieldIdx(&structType, InternalKeyword::NODES);
    return std::make_unique<StructExtractBindData>(
        *(StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

function_set RelsFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(RELS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::ANY, nullptr,
        nullptr, StructExtractFunctions::compileFunc, bindFunc, false /* isVarLength */));
    return functionSet;
}

std::unique_ptr<FunctionBindData> RelsFunction::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = StructType::getFieldIdx(&structType, InternalKeyword::RELS);
    return std::make_unique<StructExtractBindData>(
        *(StructType::getFieldTypes(&structType))[fieldIdx], fieldIdx);
}

function_set PropertiesFunction::getFunctionSet() {
    function_set functions;
    functions.push_back(make_unique<ScalarFunction>(PROPERTIES_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::STRING},
        LogicalTypeID::ANY, execFunc, nullptr, compileFunc, bindFunc, false /* isVarLength */));
    return functions;
}

std::unique_ptr<FunctionBindData> PropertiesFunction::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    if (arguments[1]->expressionType != ExpressionType::LITERAL) {
        throw BinderException(stringFormat(
            "Expected literal input as the second argument for {}().", PROPERTIES_FUNC_NAME));
    }
    auto key = ((binder::LiteralExpression&)*arguments[1]).getValue()->getValue<std::string>();
    auto listType = arguments[0]->getDataType();
    auto childType = VarListType::getChildType(&listType);
    struct_field_idx_t fieldIdx;
    if (childType->getLogicalTypeID() == LogicalTypeID::NODE ||
        childType->getLogicalTypeID() == LogicalTypeID::REL) {
        fieldIdx = StructType::getFieldIdx(childType, key);
        if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw BinderException(stringFormat("Invalid property name: {}.", key));
        }
    } else {
        throw BinderException(
            stringFormat("Cannot extract properties from {}.", listType.toString()));
    }
    auto field = StructType::getField(childType, fieldIdx);
    auto returnType = std::make_unique<LogicalType>(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(field->getType()->copy()));
    return std::make_unique<PropertiesBindData>(*returnType, fieldIdx);
}

void PropertiesFunction::compileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters[0]->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
    auto propertiesBindData = reinterpret_cast<PropertiesBindData*>(bindData);
    auto fieldVector = StructVector::getFieldVector(
        ListVector::getDataVector(parameters[0].get()), propertiesBindData->childIdx);
    ListVector::setDataVector(result.get(), fieldVector);
}

void PropertiesFunction::execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
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

function_set IsTrailFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(IS_TRAIL_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::BOOL, execFunc,
        selectFunc, nullptr, nullptr, false /* isVarLength */));
    return functionSet;
}

void IsTrailFunction::execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    UnaryPathExecutor::executeRelIDs(*parameters[0], result);
}

bool IsTrailFunction::selectFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, SelectionVector& selectionVector) {
    return UnaryPathExecutor::selectRelIDs(*parameters[0], selectionVector);
}

function_set IsACyclicFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(IS_ACYCLIC_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::BOOL, execFunc,
        selectFunc, nullptr, nullptr, false /* isVarLength */));
    return functionSet;
}

void IsACyclicFunction::execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    UnaryPathExecutor::executeNodeIDs(*parameters[0], result);
}

bool IsACyclicFunction::selectFunc(
    const std::vector<std::shared_ptr<ValueVector>>& parameters, SelectionVector& selectionVector) {
    return UnaryPathExecutor::selectNodeIDs(*parameters[0], selectionVector);
}

} // namespace function
} // namespace kuzu
