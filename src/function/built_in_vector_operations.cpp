#include "function/built_in_vector_operations.h"

#include "function/arithmetic/vector_arithmetic_operations.h"
#include "function/cast/vector_cast_operations.h"
#include "function/comparison/vector_comparison_operations.h"
#include "function/date/vector_date_operations.h"
#include "function/interval/vector_interval_operations.h"
#include "function/list/vector_list_operations.h"
#include "function/schema/vector_offset_operations.h"
#include "function/string/vector_string_operations.h"
#include "function/timestamp/vector_timestamp_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void BuiltInVectorOperations::registerVectorOperations() {
    registerComparisonOperations();
    registerArithmeticOperations();
    registerDateOperations();
    registerTimestampOperations();
    registerIntervalOperations();
    registerStringOperations();
    registerCastOperations();
    registerListOperations();
    registerInternalIDOperation();
    // register internal offset operation
    vectorOperations.insert({OFFSET_FUNC_NAME, OffsetVectorOperation::getDefinitions()});
}

bool BuiltInVectorOperations::canApplyStaticEvaluation(
    const std::string& functionName, const binder::expression_vector& children) {
    if ((functionName == CAST_TO_DATE_FUNC_NAME || functionName == CAST_TO_TIMESTAMP_FUNC_NAME ||
            functionName == CAST_TO_INTERVAL_FUNC_NAME) &&
        children[0]->expressionType == LITERAL && children[0]->dataType.typeID == STRING) {
        return true; // bind as literal
    }
    return false;
}

VectorOperationDefinition* BuiltInVectorOperations::matchFunction(
    const std::string& name, const std::vector<DataType>& inputTypes) {
    auto& functionDefinitions = vectorOperations.at(name);
    bool isOverload = functionDefinitions.size() > 1;
    std::vector<VectorOperationDefinition*> candidateFunctions;
    uint32_t minCost = UINT32_MAX;
    for (auto& functionDefinition : functionDefinitions) {
        auto cost = getFunctionCost(inputTypes, functionDefinition.get(), isOverload);
        if (cost == UINT32_MAX) {
            continue;
        }
        if (cost < minCost) {
            candidateFunctions.clear();
            candidateFunctions.push_back(functionDefinition.get());
            minCost = cost;
        } else if (cost == minCost) {
            candidateFunctions.push_back(functionDefinition.get());
        }
    }
    validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes);
    if (candidateFunctions.size() > 1) {
        return getBestMatch(candidateFunctions);
    }
    return candidateFunctions[0];
}

std::vector<std::string> BuiltInVectorOperations::getFunctionNames() {
    std::vector<std::string> result;
    for (auto& [functionName, definitions] : vectorOperations) {
        result.push_back(functionName);
    }
    return result;
}

uint32_t BuiltInVectorOperations::getCastCost(DataTypeID inputTypeID, DataTypeID targetTypeID) {
    if (inputTypeID == targetTypeID) {
        return 0;
    } else {
        if (targetTypeID == ANY) {
            // Any inputTypeID can match to type ANY
            return 0;
        }
        switch (inputTypeID) {
        case common::ANY:
            // ANY type can be any type
            return 0;
        case common::INT64:
            return castInt64(targetTypeID);
        case common::INT32:
            return castInt32(targetTypeID);
        case common::INT16:
            return castInt16(targetTypeID);
        case common::DOUBLE:
            return castDouble(targetTypeID);
        case common::FLOAT:
            return castFloat(targetTypeID);
        default:
            return UINT32_MAX;
        }
    }
}

uint32_t BuiltInVectorOperations::getCastCost(
    const DataType& inputType, const DataType& targetType) {
    if (inputType == targetType) {
        return 0;
    } else {
        switch (inputType.typeID) {
        case common::FIXED_LIST:
        case common::VAR_LIST:
            return UINT32_MAX;
        default:
            return getCastCost(inputType.typeID, targetType.typeID);
        }
    }
}

uint32_t BuiltInVectorOperations::getTargetTypeCost(common::DataTypeID typeID) {
    switch (typeID) {
    case common::INT32: {
        return 103;
    }
    case common::INT64: {
        return 101;
    }
    case common::FLOAT: {
        return 110;
    }
    case common::DOUBLE: {
        return 102;
    }
    default: {
        throw InternalException("Unsupported casting operation.");
    }
    }
}

uint32_t BuiltInVectorOperations::castInt64(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::FLOAT:
    case common::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UINT32_MAX;
    }
}

uint32_t BuiltInVectorOperations::castInt32(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::INT64:
    case common::FLOAT:
    case common::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UINT32_MAX;
    }
}

uint32_t BuiltInVectorOperations::castInt16(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::INT32:
    case common::INT64:
    case common::FLOAT:
    case common::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UINT32_MAX;
    }
}

uint32_t BuiltInVectorOperations::castDouble(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    default:
        return UINT32_MAX;
    }
}

uint32_t BuiltInVectorOperations::castFloat(common::DataTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UINT32_MAX;
    }
}

// When there is multiple candidates functions, e.g. double + int and double + double for input
// "1.5 + parameter", we prefer the one without any implicit casting i.e. double + double.
VectorOperationDefinition* BuiltInVectorOperations::getBestMatch(
    std::vector<VectorOperationDefinition*>& functions) {
    assert(functions.size() > 1);
    VectorOperationDefinition* result = nullptr;
    auto cost = UINT32_MAX;
    for (auto& function : functions) {
        std::unordered_set<DataTypeID> distinctParameterTypes;
        for (auto& parameterTypeID : function->parameterTypeIDs) {
            if (!distinctParameterTypes.contains(parameterTypeID)) {
                distinctParameterTypes.insert(parameterTypeID);
            }
        }
        if (distinctParameterTypes.size() < cost) {
            cost = distinctParameterTypes.size();
            result = function;
        }
    }
    assert(result != nullptr);
    return result;
}

uint32_t BuiltInVectorOperations::getFunctionCost(
    const std::vector<DataType>& inputTypes, VectorOperationDefinition* function, bool isOverload) {
    if (function->isVarLength) {
        assert(function->parameterTypeIDs.size() == 1);
        return matchVarLengthParameters(inputTypes, function->parameterTypeIDs[0], isOverload);
    } else {
        return matchParameters(inputTypes, function->parameterTypeIDs, isOverload);
    }
}

uint32_t BuiltInVectorOperations::matchParameters(const std::vector<DataType>& inputTypes,
    const std::vector<DataTypeID>& targetTypeIDs, bool isOverload) {
    if (inputTypes.size() != targetTypeIDs.size()) {
        return UINT32_MAX;
    }
    auto cost = 0u;
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        auto castCost = getCastCost(inputTypes[i].typeID, targetTypeIDs[i]);
        if (castCost == UINT32_MAX) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

uint32_t BuiltInVectorOperations::matchVarLengthParameters(
    const std::vector<DataType>& inputTypes, DataTypeID targetTypeID, bool isOverload) {
    auto cost = 0u;
    for (auto& inputType : inputTypes) {
        auto castCost = getCastCost(inputType.typeID, targetTypeID);
        if (castCost == UINT32_MAX) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

void BuiltInVectorOperations::validateNonEmptyCandidateFunctions(
    std::vector<VectorOperationDefinition*>& candidateFunctions, const std::string& name,
    const std::vector<DataType>& inputTypes) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& functionDefinition : vectorOperations.at(name)) {
            supportedInputsString += functionDefinition->signatureToString() + "\n";
        }
        throw BinderException("Cannot match a built-in function for given function " + name +
                              Types::dataTypesToString(inputTypes) + ". Supported inputs are\n" +
                              supportedInputsString);
    }
}

void BuiltInVectorOperations::registerComparisonOperations() {
    vectorOperations.insert({EQUALS_FUNC_NAME, EqualsVectorOperation::getDefinitions()});
    vectorOperations.insert({NOT_EQUALS_FUNC_NAME, NotEqualsVectorOperation::getDefinitions()});
    vectorOperations.insert({GREATER_THAN_FUNC_NAME, GreaterThanVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {GREATER_THAN_EQUALS_FUNC_NAME, GreaterThanEqualsVectorOperation::getDefinitions()});
    vectorOperations.insert({LESS_THAN_FUNC_NAME, LessThanVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {LESS_THAN_EQUALS_FUNC_NAME, LessThanEqualsVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerArithmeticOperations() {
    vectorOperations.insert({ADD_FUNC_NAME, AddVectorOperation::getDefinitions()});
    vectorOperations.insert({SUBTRACT_FUNC_NAME, SubtractVectorOperation::getDefinitions()});
    vectorOperations.insert({MULTIPLY_FUNC_NAME, MultiplyVectorOperation::getDefinitions()});
    vectorOperations.insert({DIVIDE_FUNC_NAME, DivideVectorOperation::getDefinitions()});
    vectorOperations.insert({MODULO_FUNC_NAME, ModuloVectorOperation::getDefinitions()});
    vectorOperations.insert({POWER_FUNC_NAME, PowerVectorOperation::getDefinitions()});

    vectorOperations.insert({ABS_FUNC_NAME, AbsVectorOperation::getDefinitions()});
    vectorOperations.insert({ACOS_FUNC_NAME, AcosVectorOperation::getDefinitions()});
    vectorOperations.insert({ASIN_FUNC_NAME, AsinVectorOperation::getDefinitions()});
    vectorOperations.insert({ATAN_FUNC_NAME, AtanVectorOperation::getDefinitions()});
    vectorOperations.insert({ATAN2_FUNC_NAME, Atan2VectorOperation::getDefinitions()});
    vectorOperations.insert({BITWISE_XOR_FUNC_NAME, BitwiseXorVectorOperation::getDefinitions()});
    vectorOperations.insert({BITWISE_AND_FUNC_NAME, BitwiseAndVectorOperation::getDefinitions()});
    vectorOperations.insert({BITWISE_OR_FUNC_NAME, BitwiseOrVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {BITSHIFT_LEFT_FUNC_NAME, BitShiftLeftVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {BITSHIFT_RIGHT_FUNC_NAME, BitShiftRightVectorOperation::getDefinitions()});
    vectorOperations.insert({CBRT_FUNC_NAME, CbrtVectorOperation::getDefinitions()});
    vectorOperations.insert({CEIL_FUNC_NAME, CeilVectorOperation::getDefinitions()});
    vectorOperations.insert({CEILING_FUNC_NAME, CeilVectorOperation::getDefinitions()});
    vectorOperations.insert({COS_FUNC_NAME, CosVectorOperation::getDefinitions()});
    vectorOperations.insert({COT_FUNC_NAME, CotVectorOperation::getDefinitions()});
    vectorOperations.insert({DEGREES_FUNC_NAME, DegreesVectorOperation::getDefinitions()});
    vectorOperations.insert({EVEN_FUNC_NAME, EvenVectorOperation::getDefinitions()});
    vectorOperations.insert({FACTORIAL_FUNC_NAME, FactorialVectorOperation::getDefinitions()});
    vectorOperations.insert({FLOOR_FUNC_NAME, FloorVectorOperation::getDefinitions()});
    vectorOperations.insert({GAMMA_FUNC_NAME, GammaVectorOperation::getDefinitions()});
    vectorOperations.insert({LGAMMA_FUNC_NAME, LgammaVectorOperation::getDefinitions()});
    vectorOperations.insert({LN_FUNC_NAME, LnVectorOperation::getDefinitions()});
    vectorOperations.insert({LOG_FUNC_NAME, LogVectorOperation::getDefinitions()});
    vectorOperations.insert({LOG2_FUNC_NAME, Log2VectorOperation::getDefinitions()});
    vectorOperations.insert({LOG10_FUNC_NAME, LogVectorOperation::getDefinitions()});
    vectorOperations.insert({NEGATE_FUNC_NAME, NegateVectorOperation::getDefinitions()});
    vectorOperations.insert({PI_FUNC_NAME, PiVectorOperation::getDefinitions()});
    vectorOperations.insert({POW_FUNC_NAME, PowerVectorOperation::getDefinitions()});
    vectorOperations.insert({RADIANS_FUNC_NAME, RadiansVectorOperation::getDefinitions()});
    vectorOperations.insert({ROUND_FUNC_NAME, RoundVectorOperation::getDefinitions()});
    vectorOperations.insert({SIN_FUNC_NAME, SinVectorOperation::getDefinitions()});
    vectorOperations.insert({SIGN_FUNC_NAME, SignVectorOperation::getDefinitions()});
    vectorOperations.insert({SQRT_FUNC_NAME, SqrtVectorOperation::getDefinitions()});
    vectorOperations.insert({TAN_FUNC_NAME, TanVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerDateOperations() {
    vectorOperations.insert({DATE_PART_FUNC_NAME, DatePartVectorOperation::getDefinitions()});
    vectorOperations.insert({DATEPART_FUNC_NAME, DatePartVectorOperation::getDefinitions()});
    vectorOperations.insert({DATE_TRUNC_FUNC_NAME, DateTruncVectorOperation::getDefinitions()});
    vectorOperations.insert({DATETRUNC_FUNC_NAME, DateTruncVectorOperation::getDefinitions()});
    vectorOperations.insert({DAYNAME_FUNC_NAME, DayNameVectorOperation::getDefinitions()});
    vectorOperations.insert({GREATEST_FUNC_NAME, GreatestVectorOperation::getDefinitions()});
    vectorOperations.insert({LAST_DAY_FUNC_NAME, LastDayVectorOperation::getDefinitions()});
    vectorOperations.insert({LEAST_FUNC_NAME, LeastVectorOperation::getDefinitions()});
    vectorOperations.insert({MAKE_DATE_FUNC_NAME, MakeDateVectorOperation::getDefinitions()});
    vectorOperations.insert({MONTHNAME_FUNC_NAME, MonthNameVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerTimestampOperations() {
    vectorOperations.insert({CENTURY_FUNC_NAME, CenturyVectorOperation::getDefinitions()});
    vectorOperations.insert({EPOCH_MS_FUNC_NAME, EpochMsVectorOperation::getDefinitions()});
    vectorOperations.insert({TO_TIMESTAMP_FUNC_NAME, ToTimestampVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerIntervalOperations() {
    vectorOperations.insert({TO_YEARS_FUNC_NAME, ToYearsVectorOperation::getDefinitions()});
    vectorOperations.insert({TO_MONTHS_FUNC_NAME, ToMonthsVectorOperation::getDefinitions()});
    vectorOperations.insert({TO_DAYS_FUNC_NAME, ToDaysVectorOperation::getDefinitions()});
    vectorOperations.insert({TO_HOURS_FUNC_NAME, ToHoursVectorOperation::getDefinitions()});
    vectorOperations.insert({TO_MINUTES_FUNC_NAME, ToMinutesVectorOperation::getDefinitions()});
    vectorOperations.insert({TO_SECONDS_FUNC_NAME, ToSecondsVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {TO_MILLISECONDS_FUNC_NAME, ToMillisecondsVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {TO_MICROSECONDS_FUNC_NAME, ToMicrosecondsVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerStringOperations() {
    vectorOperations.insert(
        {ARRAY_EXTRACT_FUNC_NAME, ArrayExtractVectorOperation::getDefinitions()});
    vectorOperations.insert({CONCAT_FUNC_NAME, ConcatVectorOperation::getDefinitions()});
    vectorOperations.insert({CONTAINS_FUNC_NAME, ContainsVectorOperation::getDefinitions()});
    vectorOperations.insert({ENDS_WITH_FUNC_NAME, EndsWithVectorOperation::getDefinitions()});
    vectorOperations.insert({RE_MATCH_FUNC_NAME, REMatchVectorOperation::getDefinitions()});
    vectorOperations.insert({LCASE_FUNC_NAME, LowerVectorOperation::getDefinitions()});
    vectorOperations.insert({LEFT_FUNC_NAME, LeftVectorOperation::getDefinitions()});
    vectorOperations.insert({LENGTH_FUNC_NAME, LengthVectorOperation::getDefinitions()});
    vectorOperations.insert({LOWER_FUNC_NAME, LowerVectorOperation::getDefinitions()});
    vectorOperations.insert({LPAD_FUNC_NAME, LpadVectorOperation::getDefinitions()});
    vectorOperations.insert({LTRIM_FUNC_NAME, LtrimVectorOperation::getDefinitions()});
    vectorOperations.insert({PREFIX_FUNC_NAME, StartsWithVectorOperation::getDefinitions()});
    vectorOperations.insert({REPEAT_FUNC_NAME, RepeatVectorOperation::getDefinitions()});
    vectorOperations.insert({REVERSE_FUNC_NAME, ReverseVectorOperation::getDefinitions()});
    vectorOperations.insert({RIGHT_FUNC_NAME, RightVectorOperation::getDefinitions()});
    vectorOperations.insert({RPAD_FUNC_NAME, RpadVectorOperation::getDefinitions()});
    vectorOperations.insert({RTRIM_FUNC_NAME, RtrimVectorOperation::getDefinitions()});
    vectorOperations.insert({STARTS_WITH_FUNC_NAME, StartsWithVectorOperation::getDefinitions()});
    vectorOperations.insert({SUBSTR_FUNC_NAME, SubStrVectorOperation::getDefinitions()});
    vectorOperations.insert({SUBSTRING_FUNC_NAME, SubStrVectorOperation::getDefinitions()});
    vectorOperations.insert({SUFFIX_FUNC_NAME, EndsWithVectorOperation::getDefinitions()});
    vectorOperations.insert({TRIM_FUNC_NAME, TrimVectorOperation::getDefinitions()});
    vectorOperations.insert({UCASE_FUNC_NAME, UpperVectorOperation::getDefinitions()});
    vectorOperations.insert({UPPER_FUNC_NAME, UpperVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerCastOperations() {
    vectorOperations.insert({CAST_TO_DATE_FUNC_NAME, CastToDateVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_TIMESTAMP_FUNC_NAME, CastToTimestampVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_INTERVAL_FUNC_NAME, CastToIntervalVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_STRING_FUNC_NAME, CastToStringVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_DOUBLE_FUNC_NAME, CastToDoubleVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_FLOAT_FUNC_NAME, CastToFloatVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_INT64_FUNC_NAME, CastToInt64VectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_INT32_FUNC_NAME, CastToInt32VectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_INT16_FUNC_NAME, CastToInt16VectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerListOperations() {
    vectorOperations.insert(
        {LIST_CREATION_FUNC_NAME, ListCreationVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_LEN_FUNC_NAME, ListLenVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_EXTRACT_FUNC_NAME, ListExtractVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_ELEMENT_FUNC_NAME, ListExtractVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_CONCAT_FUNC_NAME, ListConcatVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_CAT_FUNC_NAME, ListConcatVectorOperation::getDefinitions()});
    vectorOperations.insert({ARRAY_CONCAT_FUNC_NAME, ListConcatVectorOperation::getDefinitions()});
    vectorOperations.insert({ARRAY_CAT_FUNC_NAME, ListConcatVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_APPEND_FUNC_NAME, ListAppendVectorOperation::getDefinitions()});
    vectorOperations.insert({ARRAY_APPEND_FUNC_NAME, ListAppendVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {ARRAY_PUSH_BACK_FUNC_NAME, ListAppendVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_PREPEND_FUNC_NAME, ListPrependVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {ARRAY_PREPEND_FUNC_NAME, ListPrependVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {ARRAY_PUSH_FRONT_FUNC_NAME, ListPrependVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {LIST_POSITION_FUNC_NAME, ListPositionVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {ARRAY_POSITION_FUNC_NAME, ListPositionVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {LIST_INDEXOF_FUNC_NAME, ListPositionVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {ARRAY_INDEXOF_FUNC_NAME, ListPositionVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {LIST_CONTAINS_FUNC_NAME, ListContainsVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_HAS_FUNC_NAME, ListContainsVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {ARRAY_CONTAINS_FUNC_NAME, ListContainsVectorOperation::getDefinitions()});
    vectorOperations.insert({ARRAY_HAS_FUNC_NAME, ListContainsVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_SLICE_FUNC_NAME, ListSliceVectorOperation::getDefinitions()});
    vectorOperations.insert({ARRAY_SLICE_FUNC_NAME, ListSliceVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerInternalIDOperation() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(
        ID_FUNC_NAME, std::vector<DataTypeID>{NODE}, INTERNAL_ID, nullptr));
    definitions.push_back(make_unique<VectorOperationDefinition>(
        ID_FUNC_NAME, std::vector<DataTypeID>{REL}, INTERNAL_ID, nullptr));
    vectorOperations.insert({ID_FUNC_NAME, std::move(definitions)});
}

} // namespace function
} // namespace kuzu
