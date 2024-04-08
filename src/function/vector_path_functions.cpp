#include "function/path/vector_path_functions.h"

#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "common/vector/value_vector.h"
#include "function/path/path_function_executor.h"
#include "function/scalar_function.h"
#include "function/struct/vector_struct_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> NodesBindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = StructType::getFieldIdx(&structType, InternalKeyword::NODES);
    return std::make_unique<StructExtractBindData>(
        StructType::getFieldTypes(&structType)[fieldIdx]->copy(), fieldIdx);
}

function_set NodesFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::ANY, nullptr,
        nullptr, StructExtractFunctions::compileFunc, NodesBindFunc, false /* isVarLength */));
    return functionSet;
}

static std::unique_ptr<FunctionBindData> RelsBindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    auto structType = arguments[0]->getDataType();
    auto fieldIdx = StructType::getFieldIdx(&structType, InternalKeyword::RELS);
    return std::make_unique<StructExtractBindData>(
        StructType::getFieldTypes(&structType)[fieldIdx]->copy(), fieldIdx);
}

function_set RelsFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::ANY, nullptr,
        nullptr, StructExtractFunctions::compileFunc, RelsBindFunc, false /* isVarLength */));
    return functionSet;
}

static std::unique_ptr<FunctionBindData> PropertiesBindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    if (arguments[1]->expressionType != ExpressionType::LITERAL) {
        throw BinderException(stringFormat(
            "Expected literal input as the second argument for {}().", PropertiesFunction::name));
    }
    auto key = ((binder::LiteralExpression&)*arguments[1]).getValue()->getValue<std::string>();
    auto listType = arguments[0]->getDataType();
    auto childType = ListType::getChildType(&listType);
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
    auto returnType = LogicalType::LIST(field->getType()->copy());
    return std::make_unique<PropertiesBindData>(std::move(returnType), fieldIdx);
}

static void PropertiesCompileFunc(FunctionBindData* bindData,
    const std::vector<std::shared_ptr<ValueVector>>& parameters,
    std::shared_ptr<ValueVector>& result) {
    KU_ASSERT(parameters[0]->dataType.getPhysicalType() == PhysicalTypeID::LIST);
    auto propertiesBindData = reinterpret_cast<PropertiesBindData*>(bindData);
    auto fieldVector = StructVector::getFieldVector(ListVector::getDataVector(parameters[0].get()),
        propertiesBindData->childIdx);
    ListVector::setDataVector(result.get(), fieldVector);
}

static void PropertiesExecFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
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

function_set PropertiesFunction::getFunctionSet() {
    function_set functions;
    functions.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::STRING}, LogicalTypeID::ANY,
        PropertiesExecFunc, nullptr, PropertiesCompileFunc, PropertiesBindFunc,
        false /* isVarLength */));
    return functions;
}

static void IsTrailExecFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    UnaryPathExecutor::executeRelIDs(*parameters[0], result);
}

static bool IsTrailSelectFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    SelectionVector& selectionVector) {
    return UnaryPathExecutor::selectRelIDs(*parameters[0], selectionVector);
}

function_set IsTrailFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::BOOL,
        IsTrailExecFunc, IsTrailSelectFunc, nullptr, nullptr, false /* isVarLength */));
    return functionSet;
}

static void IsACyclicExecFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    UnaryPathExecutor::executeNodeIDs(*parameters[0], result);
}

static bool IsACyclicSelectFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    SelectionVector& selectionVector) {
    return UnaryPathExecutor::selectNodeIDs(*parameters[0], selectionVector);
}

function_set IsACyclicFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::RECURSIVE_REL}, LogicalTypeID::BOOL,
        IsACyclicExecFunc, IsACyclicSelectFunc, nullptr, nullptr, false /* isVarLength */));
    return functionSet;
}

} // namespace function
} // namespace kuzu
