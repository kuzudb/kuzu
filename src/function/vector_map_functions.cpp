#include "function/map/vector_map_functions.h"

#include "function/map/functions/map_creation_function.h"
#include "function/map/functions/map_extract_function.h"
#include "function/map/functions/map_keys_function.h"
#include "function/map/functions/map_values_function.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

static std::unique_ptr<FunctionBindData> MapCreationBindFunc(
    const binder::expression_vector& arguments, kuzu::function::Function* /*function*/) {
    auto keyType = ListType::getChildType(&arguments[0]->dataType);
    auto valueType = ListType::getChildType(&arguments[1]->dataType);
    auto resultType = LogicalType::MAP(*keyType, *valueType);
    return std::make_unique<FunctionBindData>(std::move(resultType));
}

function_set MapCreationFunctions::getFunctionSet() {
    auto execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, MapCreation>;
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::MAP,
        execFunc, nullptr, MapCreationBindFunc, false /* isVarLength */));
    return functionSet;
}

static void validateKeyType(const std::shared_ptr<binder::Expression>& mapExpression,
    const std::shared_ptr<binder::Expression>& extractKeyExpression) {
    auto mapKeyType = MapType::getKeyType(&mapExpression->dataType);
    if (*mapKeyType != extractKeyExpression->dataType) {
        throw RuntimeException("Unmatched map key type and extract key type");
    }
}

static std::unique_ptr<FunctionBindData> MapExtractBindFunc(
    const binder::expression_vector& arguments, kuzu::function::Function* function) {
    validateKeyType(arguments[0], arguments[1]);
    auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
    switch (arguments[1]->getDataType().getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            uint8_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::INT64: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            int64_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::INT32: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            int32_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::INT16: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            int16_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::INT8: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            int8_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::UINT64: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            uint64_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::UINT32: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            uint32_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::UINT16: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            uint16_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::UINT8: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            uint8_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::INT128: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            int128_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::DOUBLE: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            double, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::FLOAT: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, float,
            list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::STRING: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            ku_string_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::INTERVAL: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            interval_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::INTERNAL_ID: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            internalID_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::LIST: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            list_entry_t, list_entry_t, MapExtract>;
    } break;
    case PhysicalTypeID::STRUCT: {
        scalarFunction->execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t,
            struct_entry_t, list_entry_t, MapExtract>;
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return std::make_unique<FunctionBindData>(LogicalType::LIST(
        std::make_unique<LogicalType>(*MapType::getValueType(&arguments[0]->dataType))));
}

function_set MapExtractFunctions::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::MAP, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        nullptr, nullptr, MapExtractBindFunc, false /* isVarLength */));
    return functionSet;
}

static std::unique_ptr<FunctionBindData> MapKeysBindFunc(const binder::expression_vector& arguments,
    kuzu::function::Function* /*function*/) {
    return std::make_unique<FunctionBindData>(LogicalType::LIST(
        std::make_unique<LogicalType>(*MapType::getKeyType(&arguments[0]->dataType))));
}

function_set MapKeysFunctions::getFunctionSet() {
    auto execFunc =
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, MapKeys>;
    function_set functionSet;
    functionSet.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::MAP},
            LogicalTypeID::LIST, execFunc, nullptr, MapKeysBindFunc, false /* isVarLength */));
    return functionSet;
}

static std::unique_ptr<FunctionBindData> MapValuesBindFunc(
    const binder::expression_vector& arguments, kuzu::function::Function* /*function*/) {
    return std::make_unique<FunctionBindData>(LogicalType::LIST(
        std::make_unique<LogicalType>(*MapType::getValueType(&arguments[0]->dataType))));
}

function_set MapValuesFunctions::getFunctionSet() {
    auto execFunc =
        ScalarFunction::UnaryExecNestedTypeFunction<list_entry_t, list_entry_t, MapValues>;
    function_set functionSet;
    functionSet.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::MAP},
            LogicalTypeID::LIST, execFunc, nullptr, MapValuesBindFunc, false /* isVarLength */));
    return functionSet;
}

} // namespace function
} // namespace kuzu
