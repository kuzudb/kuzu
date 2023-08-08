#include "function/built_in_vector_functions.h"

#include "common/string_utils.h"
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
    const std::string& name, const std::vector<LogicalType>& inputTypes) {
    auto& functionDefinitions = vectorFunctions.at(name);
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
        case LogicalTypeID::ANY:
            // ANY type can be any type
            return 0;
        case LogicalTypeID::INT64:
            return castInt64(targetTypeID);
        case LogicalTypeID::INT32:
            return castInt32(targetTypeID);
        case LogicalTypeID::INT16:
            return castInt16(targetTypeID);
        case LogicalTypeID::DOUBLE:
            return castDouble(targetTypeID);
        case LogicalTypeID::FLOAT:
            return castFloat(targetTypeID);
        case LogicalTypeID::DATE:
            return castDate(targetTypeID);
        case LogicalTypeID::SERIAL:
            return castSerial(targetTypeID);
        default:
            return UNDEFINED_CAST_COST;
        }
    }
}

uint32_t BuiltInVectorFunctions::getTargetTypeCost(LogicalTypeID typeID) {
    switch (typeID) {
    case LogicalTypeID::INT32: {
        return 103;
    }
    case LogicalTypeID::INT64: {
        return 101;
    }
    case LogicalTypeID::FLOAT: {
        return 110;
    }
    case LogicalTypeID::DOUBLE: {
        return 102;
    }
    case LogicalTypeID::TIMESTAMP: {
        return 120;
    }
    default: {
        throw InternalException("Unsupported casting operation.");
    }
    }
}

uint32_t BuiltInVectorFunctions::castInt64(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castInt32(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castInt16(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castDouble(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castFloat(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castDate(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::TIMESTAMP:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorFunctions::castSerial(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT64:
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
        for (auto& functionDefinition : vectorFunctions.at(name)) {
            supportedInputsString += functionDefinition->signatureToString() + "\n";
        }
        throw BinderException("Cannot match a built-in function for given function " + name +
                              LogicalTypeUtils::dataTypesToString(inputTypes) +
                              ". Supported inputs are\n" + supportedInputsString);
    }
}

void BuiltInVectorFunctions::registerComparisonFunctions() {
    vectorFunctions.insert({EQUALS_FUNC_NAME, EqualsVectorFunction::getDefinitions()});
    vectorFunctions.insert({NOT_EQUALS_FUNC_NAME, NotEqualsVectorFunction::getDefinitions()});
    vectorFunctions.insert({GREATER_THAN_FUNC_NAME, GreaterThanVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {GREATER_THAN_EQUALS_FUNC_NAME, GreaterThanEqualsVectorFunction::getDefinitions()});
    vectorFunctions.insert({LESS_THAN_FUNC_NAME, LessThanVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {LESS_THAN_EQUALS_FUNC_NAME, LessThanEqualsVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerArithmeticFunctions() {
    vectorFunctions.insert({ADD_FUNC_NAME, AddVectorFunction::getDefinitions()});
    vectorFunctions.insert({SUBTRACT_FUNC_NAME, SubtractVectorFunction::getDefinitions()});
    vectorFunctions.insert({MULTIPLY_FUNC_NAME, MultiplyVectorFunction::getDefinitions()});
    vectorFunctions.insert({DIVIDE_FUNC_NAME, DivideVectorFunction::getDefinitions()});
    vectorFunctions.insert({MODULO_FUNC_NAME, ModuloVectorFunction::getDefinitions()});
    vectorFunctions.insert({POWER_FUNC_NAME, PowerVectorFunction::getDefinitions()});

    vectorFunctions.insert({ABS_FUNC_NAME, AbsVectorFunction::getDefinitions()});
    vectorFunctions.insert({ACOS_FUNC_NAME, AcosVectorFunction::getDefinitions()});
    vectorFunctions.insert({ASIN_FUNC_NAME, AsinVectorFunction::getDefinitions()});
    vectorFunctions.insert({ATAN_FUNC_NAME, AtanVectorFunction::getDefinitions()});
    vectorFunctions.insert({ATAN2_FUNC_NAME, Atan2VectorFunction::getDefinitions()});
    vectorFunctions.insert({BITWISE_XOR_FUNC_NAME, BitwiseXorVectorFunction::getDefinitions()});
    vectorFunctions.insert({BITWISE_AND_FUNC_NAME, BitwiseAndVectorFunction::getDefinitions()});
    vectorFunctions.insert({BITWISE_OR_FUNC_NAME, BitwiseOrVectorFunction::getDefinitions()});
    vectorFunctions.insert({BITSHIFT_LEFT_FUNC_NAME, BitShiftLeftVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {BITSHIFT_RIGHT_FUNC_NAME, BitShiftRightVectorFunction::getDefinitions()});
    vectorFunctions.insert({CBRT_FUNC_NAME, CbrtVectorFunction::getDefinitions()});
    vectorFunctions.insert({CEIL_FUNC_NAME, CeilVectorFunction::getDefinitions()});
    vectorFunctions.insert({CEILING_FUNC_NAME, CeilVectorFunction::getDefinitions()});
    vectorFunctions.insert({COS_FUNC_NAME, CosVectorFunction::getDefinitions()});
    vectorFunctions.insert({COT_FUNC_NAME, CotVectorFunction::getDefinitions()});
    vectorFunctions.insert({DEGREES_FUNC_NAME, DegreesVectorFunction::getDefinitions()});
    vectorFunctions.insert({EVEN_FUNC_NAME, EvenVectorFunction::getDefinitions()});
    vectorFunctions.insert({FACTORIAL_FUNC_NAME, FactorialVectorFunction::getDefinitions()});
    vectorFunctions.insert({FLOOR_FUNC_NAME, FloorVectorFunction::getDefinitions()});
    vectorFunctions.insert({GAMMA_FUNC_NAME, GammaVectorFunction::getDefinitions()});
    vectorFunctions.insert({LGAMMA_FUNC_NAME, LgammaVectorFunction::getDefinitions()});
    vectorFunctions.insert({LN_FUNC_NAME, LnVectorFunction::getDefinitions()});
    vectorFunctions.insert({LOG_FUNC_NAME, LogVectorFunction::getDefinitions()});
    vectorFunctions.insert({LOG2_FUNC_NAME, Log2VectorFunction::getDefinitions()});
    vectorFunctions.insert({LOG10_FUNC_NAME, LogVectorFunction::getDefinitions()});
    vectorFunctions.insert({NEGATE_FUNC_NAME, NegateVectorFunction::getDefinitions()});
    vectorFunctions.insert({PI_FUNC_NAME, PiVectorFunction::getDefinitions()});
    vectorFunctions.insert({POW_FUNC_NAME, PowerVectorFunction::getDefinitions()});
    vectorFunctions.insert({RADIANS_FUNC_NAME, RadiansVectorFunction::getDefinitions()});
    vectorFunctions.insert({ROUND_FUNC_NAME, RoundVectorFunction::getDefinitions()});
    vectorFunctions.insert({SIN_FUNC_NAME, SinVectorFunction::getDefinitions()});
    vectorFunctions.insert({SIGN_FUNC_NAME, SignVectorFunction::getDefinitions()});
    vectorFunctions.insert({SQRT_FUNC_NAME, SqrtVectorFunction::getDefinitions()});
    vectorFunctions.insert({TAN_FUNC_NAME, TanVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerDateFunctions() {
    vectorFunctions.insert({DATE_PART_FUNC_NAME, DatePartVectorFunction::getDefinitions()});
    vectorFunctions.insert({DATEPART_FUNC_NAME, DatePartVectorFunction::getDefinitions()});
    vectorFunctions.insert({DATE_TRUNC_FUNC_NAME, DateTruncVectorFunction::getDefinitions()});
    vectorFunctions.insert({DATETRUNC_FUNC_NAME, DateTruncVectorFunction::getDefinitions()});
    vectorFunctions.insert({DAYNAME_FUNC_NAME, DayNameVectorFunction::getDefinitions()});
    vectorFunctions.insert({GREATEST_FUNC_NAME, GreatestVectorFunction::getDefinitions()});
    vectorFunctions.insert({LAST_DAY_FUNC_NAME, LastDayVectorFunction::getDefinitions()});
    vectorFunctions.insert({LEAST_FUNC_NAME, LeastVectorFunction::getDefinitions()});
    vectorFunctions.insert({MAKE_DATE_FUNC_NAME, MakeDateVectorFunction::getDefinitions()});
    vectorFunctions.insert({MONTHNAME_FUNC_NAME, MonthNameVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerTimestampFunctions() {
    vectorFunctions.insert({CENTURY_FUNC_NAME, CenturyVectorFunction::getDefinitions()});
    vectorFunctions.insert({EPOCH_MS_FUNC_NAME, EpochMsVectorFunction::getDefinitions()});
    vectorFunctions.insert({TO_TIMESTAMP_FUNC_NAME, ToTimestampVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerIntervalFunctions() {
    vectorFunctions.insert({TO_YEARS_FUNC_NAME, ToYearsVectorFunction::getDefinitions()});
    vectorFunctions.insert({TO_MONTHS_FUNC_NAME, ToMonthsVectorFunction::getDefinitions()});
    vectorFunctions.insert({TO_DAYS_FUNC_NAME, ToDaysVectorFunction::getDefinitions()});
    vectorFunctions.insert({TO_HOURS_FUNC_NAME, ToHoursVectorFunction::getDefinitions()});
    vectorFunctions.insert({TO_MINUTES_FUNC_NAME, ToMinutesVectorFunction::getDefinitions()});
    vectorFunctions.insert({TO_SECONDS_FUNC_NAME, ToSecondsVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {TO_MILLISECONDS_FUNC_NAME, ToMillisecondsVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {TO_MICROSECONDS_FUNC_NAME, ToMicrosecondsVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerBlobFunctions() {
    vectorFunctions.insert({OCTET_LENGTH_FUNC_NAME, OctetLengthVectorFunctions::getDefinitions()});
    vectorFunctions.insert({ENCODE_FUNC_NAME, EncodeVectorFunctions::getDefinitions()});
    vectorFunctions.insert({DECODE_FUNC_NAME, DecodeVectorFunctions::getDefinitions()});
}

void BuiltInVectorFunctions::registerStringFunctions() {
    vectorFunctions.insert({ARRAY_EXTRACT_FUNC_NAME, ArrayExtractVectorFunction::getDefinitions()});
    vectorFunctions.insert({CONCAT_FUNC_NAME, ConcatVectorFunction::getDefinitions()});
    vectorFunctions.insert({CONTAINS_FUNC_NAME, ContainsVectorFunction::getDefinitions()});
    vectorFunctions.insert({ENDS_WITH_FUNC_NAME, EndsWithVectorFunction::getDefinitions()});
    vectorFunctions.insert({LCASE_FUNC_NAME, LowerVectorFunction::getDefinitions()});
    vectorFunctions.insert({LEFT_FUNC_NAME, LeftVectorFunction::getDefinitions()});
    vectorFunctions.insert({LENGTH_FUNC_NAME, LengthVectorFunction::getDefinitions()});
    vectorFunctions.insert({LOWER_FUNC_NAME, LowerVectorFunction::getDefinitions()});
    vectorFunctions.insert({LPAD_FUNC_NAME, LpadVectorFunction::getDefinitions()});
    vectorFunctions.insert({LTRIM_FUNC_NAME, LtrimVectorFunction::getDefinitions()});
    vectorFunctions.insert({PREFIX_FUNC_NAME, StartsWithVectorFunction::getDefinitions()});
    vectorFunctions.insert({REPEAT_FUNC_NAME, RepeatVectorFunction::getDefinitions()});
    vectorFunctions.insert({REVERSE_FUNC_NAME, ReverseVectorFunction::getDefinitions()});
    vectorFunctions.insert({RIGHT_FUNC_NAME, RightVectorFunction::getDefinitions()});
    vectorFunctions.insert({RPAD_FUNC_NAME, RpadVectorFunction::getDefinitions()});
    vectorFunctions.insert({RTRIM_FUNC_NAME, RtrimVectorFunction::getDefinitions()});
    vectorFunctions.insert({STARTS_WITH_FUNC_NAME, StartsWithVectorFunction::getDefinitions()});
    vectorFunctions.insert({SUBSTR_FUNC_NAME, SubStrVectorFunction::getDefinitions()});
    vectorFunctions.insert({SUBSTRING_FUNC_NAME, SubStrVectorFunction::getDefinitions()});
    vectorFunctions.insert({SUFFIX_FUNC_NAME, EndsWithVectorFunction::getDefinitions()});
    vectorFunctions.insert({TRIM_FUNC_NAME, TrimVectorFunction::getDefinitions()});
    vectorFunctions.insert({UCASE_FUNC_NAME, UpperVectorFunction::getDefinitions()});
    vectorFunctions.insert({UPPER_FUNC_NAME, UpperVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {REGEXP_FULL_MATCH_FUNC_NAME, RegexpFullMatchVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {REGEXP_MATCHES_FUNC_NAME, RegexpMatchesVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {REGEXP_REPLACE_FUNC_NAME, RegexpReplaceVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {REGEXP_EXTRACT_FUNC_NAME, RegexpExtractVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {REGEXP_EXTRACT_ALL_FUNC_NAME, RegexpExtractAllVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerCastFunctions() {
    vectorFunctions.insert({CAST_TO_DATE_FUNC_NAME, CastToDateVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {CAST_TO_TIMESTAMP_FUNC_NAME, CastToTimestampVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {CAST_TO_INTERVAL_FUNC_NAME, CastToIntervalVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {CAST_TO_STRING_FUNC_NAME, CastToStringVectorFunction::getDefinitions()});
    vectorFunctions.insert({CAST_TO_BLOB_FUNC_NAME, CastToBlobVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {CAST_TO_DOUBLE_FUNC_NAME, CastToDoubleVectorFunction::getDefinitions()});
    vectorFunctions.insert({CAST_TO_FLOAT_FUNC_NAME, CastToFloatVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {CAST_TO_SERIAL_FUNC_NAME, CastToSerialVectorFunction::getDefinitions()});
    vectorFunctions.insert({CAST_TO_INT64_FUNC_NAME, CastToInt64VectorFunction::getDefinitions()});
    vectorFunctions.insert({CAST_TO_INT32_FUNC_NAME, CastToInt32VectorFunction::getDefinitions()});
    vectorFunctions.insert({CAST_TO_INT16_FUNC_NAME, CastToInt16VectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerListFunctions() {
    vectorFunctions.insert({LIST_CREATION_FUNC_NAME, ListCreationVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_LEN_FUNC_NAME, ListLenVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_EXTRACT_FUNC_NAME, ListExtractVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_ELEMENT_FUNC_NAME, ListExtractVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_CONCAT_FUNC_NAME, ListConcatVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_CAT_FUNC_NAME, ListConcatVectorFunction::getDefinitions()});
    vectorFunctions.insert({ARRAY_CONCAT_FUNC_NAME, ListConcatVectorFunction::getDefinitions()});
    vectorFunctions.insert({ARRAY_CAT_FUNC_NAME, ListConcatVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_APPEND_FUNC_NAME, ListAppendVectorFunction::getDefinitions()});
    vectorFunctions.insert({ARRAY_APPEND_FUNC_NAME, ListAppendVectorFunction::getDefinitions()});
    vectorFunctions.insert({ARRAY_PUSH_BACK_FUNC_NAME, ListAppendVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_PREPEND_FUNC_NAME, ListPrependVectorFunction::getDefinitions()});
    vectorFunctions.insert({ARRAY_PREPEND_FUNC_NAME, ListPrependVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {ARRAY_PUSH_FRONT_FUNC_NAME, ListPrependVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_POSITION_FUNC_NAME, ListPositionVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {ARRAY_POSITION_FUNC_NAME, ListPositionVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_INDEXOF_FUNC_NAME, ListPositionVectorFunction::getDefinitions()});
    vectorFunctions.insert({ARRAY_INDEXOF_FUNC_NAME, ListPositionVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_CONTAINS_FUNC_NAME, ListContainsVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_HAS_FUNC_NAME, ListContainsVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {ARRAY_CONTAINS_FUNC_NAME, ListContainsVectorFunction::getDefinitions()});
    vectorFunctions.insert({ARRAY_HAS_FUNC_NAME, ListContainsVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_SLICE_FUNC_NAME, ListSliceVectorFunction::getDefinitions()});
    vectorFunctions.insert({ARRAY_SLICE_FUNC_NAME, ListSliceVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_SORT_FUNC_NAME, ListSortVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {LIST_REVERSE_SORT_FUNC_NAME, ListReverseSortVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_SUM_FUNC_NAME, ListSumVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_DISTINCT_FUNC_NAME, ListDistinctVectorFunction::getDefinitions()});
    vectorFunctions.insert({LIST_UNIQUE_FUNC_NAME, ListUniqueVectorFunction::getDefinitions()});
    vectorFunctions.insert(
        {LIST_ANY_VALUE_FUNC_NAME, ListAnyValueVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerStructFunctions() {
    vectorFunctions.insert({STRUCT_PACK_FUNC_NAME, StructPackVectorFunctions::getDefinitions()});
    vectorFunctions.insert(
        {STRUCT_EXTRACT_FUNC_NAME, StructExtractVectorFunctions::getDefinitions()});
}

void BuiltInVectorFunctions::registerMapFunctions() {
    vectorFunctions.insert({MAP_CREATION_FUNC_NAME, MapCreationVectorFunctions::getDefinitions()});
    vectorFunctions.insert({MAP_EXTRACT_FUNC_NAME, MapExtractVectorFunctions::getDefinitions()});
    vectorFunctions.insert({ELEMENT_AT_FUNC_NAME, MapExtractVectorFunctions::getDefinitions()});
    vectorFunctions.insert({CARDINALITY_FUNC_NAME, ListLenVectorFunction::getDefinitions()});
    vectorFunctions.insert({MAP_KEYS_FUNC_NAME, MapKeysVectorFunctions::getDefinitions()});
    vectorFunctions.insert({MAP_VALUES_FUNC_NAME, MapValuesVectorFunctions::getDefinitions()});
}

void BuiltInVectorFunctions::registerUnionFunctions() {
    vectorFunctions.insert({UNION_VALUE_FUNC_NAME, UnionValueVectorFunction::getDefinitions()});
    vectorFunctions.insert({UNION_TAG_FUNC_NAME, UnionTagVectorFunction::getDefinitions()});
    vectorFunctions.insert({UNION_EXTRACT_FUNC_NAME, UnionExtractVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerNodeRelFunctions() {
    vectorFunctions.insert({OFFSET_FUNC_NAME, OffsetVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::registerPathFunctions() {
    vectorFunctions.insert({NODES_FUNC_NAME, NodesVectorFunction::getDefinitions()});
    vectorFunctions.insert({RELS_FUNC_NAME, RelsVectorFunction::getDefinitions()});
    vectorFunctions.insert({PROPERTIES_FUNC_NAME, PropertiesVectorFunction::getDefinitions()});
    vectorFunctions.insert({IS_TRAIL_FUNC_NAME, IsTrailVectorFunction::getDefinitions()});
    vectorFunctions.insert({IS_ACYCLIC_FUNC_NAME, IsACyclicVectorFunction::getDefinitions()});
}

void BuiltInVectorFunctions::addFunction(
    std::string name, function::vector_function_definitions definitions) {
    if (vectorFunctions.contains(name)) {
        throw CatalogException{StringUtils::string_format("function {} already exists.", name)};
    }
    vectorFunctions.emplace(std::move(name), std::move(definitions));
}

} // namespace function
} // namespace kuzu
