#include "function/built_in_vector_operations.h"

#include "function/arithmetic/vector_arithmetic_operations.h"
#include "function/cast/vector_cast_operations.h"
#include "function/comparison/vector_comparison_operations.h"
#include "function/date/vector_date_operations.h"
#include "function/interval/vector_interval_operations.h"
#include "function/list/vector_list_operations.h"
#include "function/string/vector_string_operations.h"
#include "function/timestamp/vector_timestamp_operations.h"

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
}

bool BuiltInVectorOperations::canApplyStaticEvaluation(
    const string& functionName, const expression_vector& children) {
    if (functionName == ID_FUNC_NAME) {
        return true; // bind as property
    }
    if ((functionName == CAST_TO_DATE_FUNC_NAME || functionName == CAST_TO_TIMESTAMP_FUNC_NAME ||
            functionName == CAST_TO_INTERVAL_FUNC_NAME) &&
        children[0]->expressionType == LITERAL && children[0]->dataType.typeID == STRING) {
        return true; // bind as literal
    }
    return false;
}

VectorOperationDefinition* BuiltInVectorOperations::matchFunction(
    const string& name, const vector<DataType>& inputTypes) {
    auto& functionDefinitions = vectorOperations.at(name);
    bool isOverload = functionDefinitions.size() > 1;
    vector<VectorOperationDefinition*> candidateFunctions;
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

vector<string> BuiltInVectorOperations::getFunctionNames() {
    vector<string> result;
    for (auto& [functionName, definitions] : vectorOperations) {
        result.push_back(functionName);
    }
    return result;
}

// When there is multiple candidates functions, e.g. double + int and double + double for input
// "1.5 + parameter", we prefer the one without any implicit casting i.e. double + double.
VectorOperationDefinition* BuiltInVectorOperations::getBestMatch(
    vector<VectorOperationDefinition*>& functions) {
    assert(functions.size() > 1);
    VectorOperationDefinition* result = nullptr;
    auto cost = UINT32_MAX;
    for (auto& function : functions) {
        unordered_set<DataTypeID> distinctParameterTypes;
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
    const vector<DataType>& inputTypes, VectorOperationDefinition* function, bool isOverload) {
    if (function->isVarLength) {
        assert(function->parameterTypeIDs.size() == 1);
        return matchVarLengthParameters(inputTypes, function->parameterTypeIDs[0], isOverload);
    } else {
        return matchParameters(inputTypes, function->parameterTypeIDs, isOverload);
    }
}

uint32_t BuiltInVectorOperations::matchParameters(
    const vector<DataType>& inputTypes, const vector<DataTypeID>& targetTypeIDs, bool isOverload) {
    if (inputTypes.size() != targetTypeIDs.size()) {
        return UINT32_MAX;
    }
    auto cost = 0u;
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        auto castCost = castRules(inputTypes[i].typeID, targetTypeIDs[i]);
        if (castCost == UINT32_MAX) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

uint32_t BuiltInVectorOperations::matchVarLengthParameters(
    const vector<DataType>& inputTypes, DataTypeID targetTypeID, bool isOverload) {
    auto cost = 0u;
    for (auto& inputType : inputTypes) {
        auto castCost = castRules(inputType.typeID, targetTypeID);
        if (castCost == UINT32_MAX) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

uint32_t BuiltInVectorOperations::castRules(DataTypeID inputTypeID, DataTypeID targetTypeID) {
    if (inputTypeID == ANY) {
        // ANY type can be any type
        return 0;
    }
    if (targetTypeID == ANY) {
        // Any inputTypeID can match to type ANY
        return 0;
    }
    if (inputTypeID != targetTypeID) {
        // Unable to cast
        return UINT32_MAX;
    }
    return 0; // no cast needed
}

void BuiltInVectorOperations::validateNonEmptyCandidateFunctions(
    vector<VectorOperationDefinition*>& candidateFunctions, const string& name,
    const vector<DataType>& inputTypes) {
    if (candidateFunctions.empty()) {
        string supportedInputsString;
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
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    definitions.push_back(make_unique<VectorOperationDefinition>(
        ID_FUNC_NAME, vector<DataTypeID>{NODE}, NODE_ID, nullptr));
    definitions.push_back(make_unique<VectorOperationDefinition>(
        ID_FUNC_NAME, vector<DataTypeID>{REL}, INT64, nullptr));
    vectorOperations.insert({ID_FUNC_NAME, move(definitions)});
}

} // namespace function
} // namespace kuzu
