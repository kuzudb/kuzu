#include "function/built_in_vector_functions.h"

#include "function/arithmetic/vector_arithmetic_functions.h"
#include "function/blob/vector_blob_functions.h"
#include "function/cast/vector_cast_functions.h"
#include "function/comparison/vector_comparison_functions.h"
#include "function/date/vector_date_functions.h"
#include "function/interval/vector_interval_functions.h"
#include "function/list/vector_list_functions.h"
#include "function/map/vector_map_functions.h"
#include "function/path/vector_path_functions.h"
#include "function/schema/vector_node_rel_functions.h"
#include "function/string/vector_string_functions.h"
#include "function/struct/vector_struct_functions.h"
#include "function/timestamp/vector_timestamp_functions.h"
#include "function/union/vector_union_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void BuiltInVectorFunctions::registerVectorFunctions() {
    registerComparisonFunctions();
    registerArithmeticFunctions();
    registerDateFunctions();
    registerTimestampFunctions();
    registerIntervalFunctions();
    registerStringFunctions();
    registerCastFunctions();
    registerListFunctions();
    registerStructFunctions();
    registerMapFunctions();
    registerUnionFunctions();
    registerNodeRelFunctions();
    registerPathFunctions();
    registerBlobFunctions();
}

bool BuiltInVectorFunctions::canApplyStaticEvaluation(
    const std::string& functionName, const binder::expression_vector& children) {
    if ((functionName == CAST_TO_DATE_FUNC_NAME || functionName == CAST_TO_TIMESTAMP_FUNC_NAME ||
            functionName == CAST_TO_INTERVAL_FUNC_NAME) &&
        children[0]->expressionType == LITERAL &&
        children[0]->dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        return true; // bind as literal
    }
    return false;
}

VectorFunctionDefinition* BuiltInVectorFunctions::matchVectorFunction(
    const std::string& name, const std::vector<common::LogicalType>& inputTypes) {
    auto& functionDefinitions = VectorFunctions.at(name);
    bool isOverload = functionDefinitions.size() > 1;
    std::vector<VectorFunctionDefinition*> candidateFunctions;
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

uint32_t BuiltInVectorFunctions::getCastCost(
    LogicalTypeID inputTypeID, LogicalTypeID targetTypeID) {
    if (inputTypeID == targetTypeID) {
        return 0;
    } else {
        if (targetTypeID == LogicalTypeID::ANY) {
            // Any inputTypeID can match to type ANY
            return 0;
        }
        switch (inputTypeID) {
        case common::LogicalTypeID::ANY:
            // ANY type can be any type
            return 0;
        case common::LogicalTypeID::INT64:
            return castInt64(targetTypeID);
        case common::LogicalTypeID::INT32:
            return castInt32(targetTypeID);
        case common::LogicalTypeID::INT16:
            return castInt16(targetTypeID);
        case common::LogicalTypeID::DOUBLE:
            return castDouble(targetTypeID);
        case common::LogicalTypeID::FLOAT:
            return castFloat(targetTypeID);
        case common::LogicalTypeID::DATE:
            return castDate(targetTypeID);
        case common::LogicalTypeID::SERIAL:
            return castSerial(targetTypeID);
        default:
            return UNDEFINED_CAST_COST;
        }
    }
}

uint32_t BuiltInVectorFunctions::getTargetTypeCost(common::LogicalTypeID typeID) {
    switch (typeID) {
    case common::LogicalTypeID::INT32: {
        return 103;
    }
    case common::LogicalTypeID::INT64: {
        return 101;
    }
    case common::LogicalTypeID::FLOAT: {
        return 110;
    }
    case common::LogicalTypeID::DOUBLE: {
        return 102;
    }
    case common::LogicalTypeID::TIMESTAMP: {
        return 120;
    }
    default: {
        throw InternalException("Unsupported casting operation.");
    }
    }
}

uint32_t BuiltInVectorFunctions::castInt64(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::FLOAT:
    case common::LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castInt32(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::INT64:
    case common::LogicalTypeID::FLOAT:
    case common::LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castInt16(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::INT32:
    case common::LogicalTypeID::INT64:
    case common::LogicalTypeID::FLOAT:
    case common::LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castDouble(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castFloat(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castDate(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::TIMESTAMP:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castSerial(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::INT64:
        return 0;
    default:
        return castInt64(targetTypeID);
    }
}

// When there is multiple candidates functions, e.g. double + int and double + double for input
// "1.5 + parameter", we prefer the one without any implicit casting i.e. double + double.
VectorFunctionDefinition* BuiltInVectorFunctions::getBestMatch(
    std::vector<VectorFunctionDefinition*>& functions) {
    assert(functions.size() > 1);
    VectorFunctionDefinition* result = nullptr;
    auto cost = UNDEFINED_CAST_COST;
    for (auto& function : functions) {
        std::unordered_set<LogicalTypeID> distinctParameterTypes;
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

uint32_t BuiltInVectorFunctions::getFunctionCost(const std::vector<LogicalType>& inputTypes,
    VectorFunctionDefinition* function, bool isOverload) {
    if (function->isVarLength) {
        assert(function->parameterTypeIDs.size() == 1);
        return matchVarLengthParameters(inputTypes, function->parameterTypeIDs[0], isOverload);
    } else {
        return matchParameters(inputTypes, function->parameterTypeIDs, isOverload);
    }
}

uint32_t BuiltInVectorFunctions::matchParameters(const std::vector<LogicalType>& inputTypes,
    const std::vector<LogicalTypeID>& targetTypeIDs, bool isOverload) {
    if (inputTypes.size() != targetTypeIDs.size()) {
        return UINT32_MAX;
    }
    auto cost = 0u;
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        auto castCost = getCastCost(inputTypes[i].getLogicalTypeID(), targetTypeIDs[i]);
        if (castCost == UNDEFINED_CAST_COST) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

uint32_t BuiltInVectorFunctions::matchVarLengthParameters(
    const std::vector<LogicalType>& inputTypes, LogicalTypeID targetTypeID, bool isOverload) {
    auto cost = 0u;
    for (auto& inputType : inputTypes) {
        auto castCost = getCastCost(inputType.getLogicalTypeID(), targetTypeID);
        if (castCost == UNDEFINED_CAST_COST) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

void BuiltInVectorFunctions::validateNonEmptyCandidateFunctions(
    std::vector<VectorFunctionDefinition*>& candidateFunctions, const std::string& name,
    const std::vector<LogicalType>& inputTypes) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& functionDefinition : VectorFunctions.at(name)) {
            supportedInputsString += functionDefinition->signatureToString() + "\n";
        }
        throw BinderException("Cannot match a built-in function for given function " + name +
                              LogicalTypeUtils::dataTypesToString(inputTypes) +
                              ". Supported inputs are\n" + supportedInputsString);
    }
}

void BuiltInVectorFunctions::registerComparisonFunctions() {
    VectorFunctions.insert({EQUALS_FUNC_NAME, EqualsVectorFunction::getDefinitions()});
    VectorFunctions.insert({NOT_EQUALS_FUNC_NAME, NotEqualsVectorFunction::getDefinitions()});
    VectorFunctions.insert({GREATER_THAN_FUNC_NAME, GreaterThanVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {GREATER_THAN_EQUALS_FUNC_NAME, GreaterThanEqualsVectorFunction::getDefinitions()});
    VectorFunctions.insert({LESS_THAN_FUNC_NAME, LessThanVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {LESS_THAN_EQUALS_FUNC_NAME, LessThanEqualsVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerArithmeticFunctions() {
    VectorFunctions.insert({ADD_FUNC_NAME, AddVectorFunction::getDefinitions()});
    VectorFunctions.insert({SUBTRACT_FUNC_NAME, SubtractVectorFunction::getDefinitions()});
    VectorFunctions.insert({MULTIPLY_FUNC_NAME, MultiplyVectorFunction::getDefinitions()});
    VectorFunctions.insert({DIVIDE_FUNC_NAME, DivideVectorFunction::getDefinitions()});
    VectorFunctions.insert({MODULO_FUNC_NAME, ModuloVectorFunction::getDefinitions()});
    VectorFunctions.insert({POWER_FUNC_NAME, PowerVectorFunction::getDefinitions()});

    VectorFunctions.insert({ABS_FUNC_NAME, AbsVectorFunction::getDefinitions()});
    VectorFunctions.insert({ACOS_FUNC_NAME, AcosVectorFunction::getDefinitions()});
    VectorFunctions.insert({ASIN_FUNC_NAME, AsinVectorFunction::getDefinitions()});
    VectorFunctions.insert({ATAN_FUNC_NAME, AtanVectorFunction::getDefinitions()});
    VectorFunctions.insert({ATAN2_FUNC_NAME, Atan2VectorFunction::getDefinitions()});
    VectorFunctions.insert({BITWISE_XOR_FUNC_NAME, BitwiseXorVectorFunction::getDefinitions()});
    VectorFunctions.insert({BITWISE_AND_FUNC_NAME, BitwiseAndVectorFunction::getDefinitions()});
    VectorFunctions.insert({BITWISE_OR_FUNC_NAME, BitwiseOrVectorFunction::getDefinitions()});
    VectorFunctions.insert({BITSHIFT_LEFT_FUNC_NAME, BitShiftLeftVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {BITSHIFT_RIGHT_FUNC_NAME, BitShiftRightVectorFunction::getDefinitions()});
    VectorFunctions.insert({CBRT_FUNC_NAME, CbrtVectorFunction::getDefinitions()});
    VectorFunctions.insert({CEIL_FUNC_NAME, CeilVectorFunction::getDefinitions()});
    VectorFunctions.insert({CEILING_FUNC_NAME, CeilVectorFunction::getDefinitions()});
    VectorFunctions.insert({COS_FUNC_NAME, CosVectorFunction::getDefinitions()});
    VectorFunctions.insert({COT_FUNC_NAME, CotVectorFunction::getDefinitions()});
    VectorFunctions.insert({DEGREES_FUNC_NAME, DegreesVectorFunction::getDefinitions()});
    VectorFunctions.insert({EVEN_FUNC_NAME, EvenVectorFunction::getDefinitions()});
    VectorFunctions.insert({FACTORIAL_FUNC_NAME, FactorialVectorFunction::getDefinitions()});
    VectorFunctions.insert({FLOOR_FUNC_NAME, FloorVectorFunction::getDefinitions()});
    VectorFunctions.insert({GAMMA_FUNC_NAME, GammaVectorFunction::getDefinitions()});
    VectorFunctions.insert({LGAMMA_FUNC_NAME, LgammaVectorFunction::getDefinitions()});
    VectorFunctions.insert({LN_FUNC_NAME, LnVectorFunction::getDefinitions()});
    VectorFunctions.insert({LOG_FUNC_NAME, LogVectorFunction::getDefinitions()});
    VectorFunctions.insert({LOG2_FUNC_NAME, Log2VectorFunction::getDefinitions()});
    VectorFunctions.insert({LOG10_FUNC_NAME, LogVectorFunction::getDefinitions()});
    VectorFunctions.insert({NEGATE_FUNC_NAME, NegateVectorFunction::getDefinitions()});
    VectorFunctions.insert({PI_FUNC_NAME, PiVectorFunction::getDefinitions()});
    VectorFunctions.insert({POW_FUNC_NAME, PowerVectorFunction::getDefinitions()});
    VectorFunctions.insert({RADIANS_FUNC_NAME, RadiansVectorFunction::getDefinitions()});
    VectorFunctions.insert({ROUND_FUNC_NAME, RoundVectorFunction::getDefinitions()});
    VectorFunctions.insert({SIN_FUNC_NAME, SinVectorFunction::getDefinitions()});
    VectorFunctions.insert({SIGN_FUNC_NAME, SignVectorFunction::getDefinitions()});
    VectorFunctions.insert({SQRT_FUNC_NAME, SqrtVectorFunction::getDefinitions()});
    VectorFunctions.insert({TAN_FUNC_NAME, TanVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerDateFunctions() {
    VectorFunctions.insert({DATE_PART_FUNC_NAME, DatePartVectorFunction::getDefinitions()});
    VectorFunctions.insert({DATEPART_FUNC_NAME, DatePartVectorFunction::getDefinitions()});
    VectorFunctions.insert({DATE_TRUNC_FUNC_NAME, DateTruncVectorFunction::getDefinitions()});
    VectorFunctions.insert({DATETRUNC_FUNC_NAME, DateTruncVectorFunction::getDefinitions()});
    VectorFunctions.insert({DAYNAME_FUNC_NAME, DayNameVectorFunction::getDefinitions()});
    VectorFunctions.insert({GREATEST_FUNC_NAME, GreatestVectorFunction::getDefinitions()});
    VectorFunctions.insert({LAST_DAY_FUNC_NAME, LastDayVectorFunction::getDefinitions()});
    VectorFunctions.insert({LEAST_FUNC_NAME, LeastVectorFunction::getDefinitions()});
    VectorFunctions.insert({MAKE_DATE_FUNC_NAME, MakeDateVectorFunction::getDefinitions()});
    VectorFunctions.insert({MONTHNAME_FUNC_NAME, MonthNameVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerTimestampFunctions() {
    VectorFunctions.insert({CENTURY_FUNC_NAME, CenturyVectorFunction::getDefinitions()});
    VectorFunctions.insert({EPOCH_MS_FUNC_NAME, EpochMsVectorFunction::getDefinitions()});
    VectorFunctions.insert({TO_TIMESTAMP_FUNC_NAME, ToTimestampVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerIntervalFunctions() {
    VectorFunctions.insert({TO_YEARS_FUNC_NAME, ToYearsVectorFunction::getDefinitions()});
    VectorFunctions.insert({TO_MONTHS_FUNC_NAME, ToMonthsVectorFunction::getDefinitions()});
    VectorFunctions.insert({TO_DAYS_FUNC_NAME, ToDaysVectorFunction::getDefinitions()});
    VectorFunctions.insert({TO_HOURS_FUNC_NAME, ToHoursVectorFunction::getDefinitions()});
    VectorFunctions.insert({TO_MINUTES_FUNC_NAME, ToMinutesVectorFunction::getDefinitions()});
    VectorFunctions.insert({TO_SECONDS_FUNC_NAME, ToSecondsVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {TO_MILLISECONDS_FUNC_NAME, ToMillisecondsVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {TO_MICROSECONDS_FUNC_NAME, ToMicrosecondsVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerBlobFunctions() {
    VectorFunctions.insert({OCTET_LENGTH_FUNC_NAME, OctetLengthVectorFunctions::getDefinitions()});
    VectorFunctions.insert({ENCODE_FUNC_NAME, EncodeVectorFunctions::getDefinitions()});
    VectorFunctions.insert({DECODE_FUNC_NAME, DecodeVectorFunctions::getDefinitions()});
}

void BuiltInVectorFunctions::registerStringFunctions() {
    VectorFunctions.insert({ARRAY_EXTRACT_FUNC_NAME, ArrayExtractVectorFunction::getDefinitions()});
    VectorFunctions.insert({CONCAT_FUNC_NAME, ConcatVectorFunction::getDefinitions()});
    VectorFunctions.insert({CONTAINS_FUNC_NAME, ContainsVectorFunction::getDefinitions()});
    VectorFunctions.insert({ENDS_WITH_FUNC_NAME, EndsWithVectorFunction::getDefinitions()});
    VectorFunctions.insert({LCASE_FUNC_NAME, LowerVectorFunction::getDefinitions()});
    VectorFunctions.insert({LEFT_FUNC_NAME, LeftVectorFunction::getDefinitions()});
    VectorFunctions.insert({LENGTH_FUNC_NAME, LengthVectorFunction::getDefinitions()});
    VectorFunctions.insert({LOWER_FUNC_NAME, LowerVectorFunction::getDefinitions()});
    VectorFunctions.insert({LPAD_FUNC_NAME, LpadVectorFunction::getDefinitions()});
    VectorFunctions.insert({LTRIM_FUNC_NAME, LtrimVectorFunction::getDefinitions()});
    VectorFunctions.insert({PREFIX_FUNC_NAME, StartsWithVectorFunction::getDefinitions()});
    VectorFunctions.insert({REPEAT_FUNC_NAME, RepeatVectorFunction::getDefinitions()});
    VectorFunctions.insert({REVERSE_FUNC_NAME, ReverseVectorFunction::getDefinitions()});
    VectorFunctions.insert({RIGHT_FUNC_NAME, RightVectorFunction::getDefinitions()});
    VectorFunctions.insert({RPAD_FUNC_NAME, RpadVectorFunction::getDefinitions()});
    VectorFunctions.insert({RTRIM_FUNC_NAME, RtrimVectorFunction::getDefinitions()});
    VectorFunctions.insert({STARTS_WITH_FUNC_NAME, StartsWithVectorFunction::getDefinitions()});
    VectorFunctions.insert({SUBSTR_FUNC_NAME, SubStrVectorFunction::getDefinitions()});
    VectorFunctions.insert({SUBSTRING_FUNC_NAME, SubStrVectorFunction::getDefinitions()});
    VectorFunctions.insert({SUFFIX_FUNC_NAME, EndsWithVectorFunction::getDefinitions()});
    VectorFunctions.insert({TRIM_FUNC_NAME, TrimVectorFunction::getDefinitions()});
    VectorFunctions.insert({UCASE_FUNC_NAME, UpperVectorFunction::getDefinitions()});
    VectorFunctions.insert({UPPER_FUNC_NAME, UpperVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {REGEXP_FULL_MATCH_FUNC_NAME, RegexpFullMatchVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {REGEXP_MATCHES_FUNC_NAME, RegexpMatchesVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {REGEXP_REPLACE_FUNC_NAME, RegexpReplaceVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {REGEXP_EXTRACT_FUNC_NAME, RegexpExtractVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {REGEXP_EXTRACT_ALL_FUNC_NAME, RegexpExtractAllVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerCastFunctions() {
    VectorFunctions.insert({CAST_TO_DATE_FUNC_NAME, CastToDateVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {CAST_TO_TIMESTAMP_FUNC_NAME, CastToTimestampVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {CAST_TO_INTERVAL_FUNC_NAME, CastToIntervalVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {CAST_TO_STRING_FUNC_NAME, CastToStringVectorFunction::getDefinitions()});
    VectorFunctions.insert({CAST_TO_BLOB_FUNC_NAME, CastToBlobVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {CAST_TO_DOUBLE_FUNC_NAME, CastToDoubleVectorFunction::getDefinitions()});
    VectorFunctions.insert({CAST_TO_FLOAT_FUNC_NAME, CastToFloatVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {CAST_TO_SERIAL_FUNC_NAME, CastToSerialVectorFunction::getDefinitions()});
    VectorFunctions.insert({CAST_TO_INT64_FUNC_NAME, CastToInt64VectorFunction::getDefinitions()});
    VectorFunctions.insert({CAST_TO_INT32_FUNC_NAME, CastToInt32VectorFunction::getDefinitions()});
    VectorFunctions.insert({CAST_TO_INT16_FUNC_NAME, CastToInt16VectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerListFunctions() {
    VectorFunctions.insert({LIST_CREATION_FUNC_NAME, ListCreationVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_LEN_FUNC_NAME, ListLenVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_EXTRACT_FUNC_NAME, ListExtractVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_ELEMENT_FUNC_NAME, ListExtractVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_CONCAT_FUNC_NAME, ListConcatVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_CAT_FUNC_NAME, ListConcatVectorFunction::getDefinitions()});
    VectorFunctions.insert({ARRAY_CONCAT_FUNC_NAME, ListConcatVectorFunction::getDefinitions()});
    VectorFunctions.insert({ARRAY_CAT_FUNC_NAME, ListConcatVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_APPEND_FUNC_NAME, ListAppendVectorFunction::getDefinitions()});
    VectorFunctions.insert({ARRAY_APPEND_FUNC_NAME, ListAppendVectorFunction::getDefinitions()});
    VectorFunctions.insert({ARRAY_PUSH_BACK_FUNC_NAME, ListAppendVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_PREPEND_FUNC_NAME, ListPrependVectorFunction::getDefinitions()});
    VectorFunctions.insert({ARRAY_PREPEND_FUNC_NAME, ListPrependVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {ARRAY_PUSH_FRONT_FUNC_NAME, ListPrependVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_POSITION_FUNC_NAME, ListPositionVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {ARRAY_POSITION_FUNC_NAME, ListPositionVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_INDEXOF_FUNC_NAME, ListPositionVectorFunction::getDefinitions()});
    VectorFunctions.insert({ARRAY_INDEXOF_FUNC_NAME, ListPositionVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_CONTAINS_FUNC_NAME, ListContainsVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_HAS_FUNC_NAME, ListContainsVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {ARRAY_CONTAINS_FUNC_NAME, ListContainsVectorFunction::getDefinitions()});
    VectorFunctions.insert({ARRAY_HAS_FUNC_NAME, ListContainsVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_SLICE_FUNC_NAME, ListSliceVectorFunction::getDefinitions()});
    VectorFunctions.insert({ARRAY_SLICE_FUNC_NAME, ListSliceVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_SORT_FUNC_NAME, ListSortVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {LIST_REVERSE_SORT_FUNC_NAME, ListReverseSortVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_SUM_FUNC_NAME, ListSumVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_DISTINCT_FUNC_NAME, ListDistinctVectorFunction::getDefinitions()});
    VectorFunctions.insert({LIST_UNIQUE_FUNC_NAME, ListUniqueVectorFunction::getDefinitions()});
    VectorFunctions.insert(
        {LIST_ANY_VALUE_FUNC_NAME, ListAnyValueVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerStructFunctions() {
    VectorFunctions.insert({STRUCT_PACK_FUNC_NAME, StructPackVectorFunctions::getDefinitions()});
    VectorFunctions.insert(
        {STRUCT_EXTRACT_FUNC_NAME, StructExtractVectorFunctions::getDefinitions()});
}

void BuiltInVectorFunctions::registerMapFunctions() {
    VectorFunctions.insert({MAP_CREATION_FUNC_NAME, MapCreationVectorFunctions::getDefinitions()});
    VectorFunctions.insert({MAP_EXTRACT_FUNC_NAME, MapExtractVectorFunctions::getDefinitions()});
    VectorFunctions.insert({ELEMENT_AT_FUNC_NAME, MapExtractVectorFunctions::getDefinitions()});
    VectorFunctions.insert({CARDINALITY_FUNC_NAME, ListLenVectorFunction::getDefinitions()});
    VectorFunctions.insert({MAP_KEYS_FUNC_NAME, MapKeysVectorFunctions::getDefinitions()});
    VectorFunctions.insert({MAP_VALUES_FUNC_NAME, MapValuesVectorFunctions::getDefinitions()});
}

void BuiltInVectorFunctions::registerUnionFunctions() {
    VectorFunctions.insert({UNION_VALUE_FUNC_NAME, UnionValueVectorFunction::getDefinitions()});
    VectorFunctions.insert({UNION_TAG_FUNC_NAME, UnionTagVectorFunction::getDefinitions()});
    VectorFunctions.insert({UNION_EXTRACT_FUNC_NAME, UnionExtractVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerNodeRelFunctions() {
    VectorFunctions.insert({OFFSET_FUNC_NAME, OffsetVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerPathFunctions() {
    VectorFunctions.insert({NODES_FUNC_NAME, NodesVectorFunction::getDefinitions()});
    VectorFunctions.insert({RELS_FUNC_NAME, RelsVectorFunction::getDefinitions()});
    VectorFunctions.insert({PROPERTIES_FUNC_NAME, PropertiesVectorFunction::getDefinitions()});
}

} // namespace function
} // namespace kuzu
