#include "function/built_in_vector_operations.h"

#include "function/arithmetic/vector_arithmetic_operations.h"
#include "function/blob/vector_blob_operations.h"
#include "function/cast/vector_cast_operations.h"
#include "function/comparison/vector_comparison_operations.h"
#include "function/date/vector_date_operations.h"
#include "function/interval/vector_interval_operations.h"
#include "function/list/vector_list_operations.h"
#include "function/map/vector_map_operations.h"
#include "function/schema/vector_node_rel_operations.h"
#include "function/string/vector_string_operations.h"
#include "function/struct/vector_struct_operations.h"
#include "function/timestamp/vector_timestamp_operations.h"
#include "function/union/vector_union_operations.h"

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
    registerStructOperations();
    registerMapOperations();
    registerUnionOperations();
    registerNodeRelOperations();
    registerBlobOperations();
}

bool BuiltInVectorOperations::canApplyStaticEvaluation(
    const std::string& functionName, const binder::expression_vector& children) {
    if ((functionName == CAST_TO_DATE_FUNC_NAME || functionName == CAST_TO_TIMESTAMP_FUNC_NAME ||
            functionName == CAST_TO_INTERVAL_FUNC_NAME) &&
        children[0]->expressionType == LITERAL &&
        children[0]->dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
        return true; // bind as literal
    }
    return false;
}

VectorOperationDefinition* BuiltInVectorOperations::matchFunction(
    const std::string& name, const std::vector<LogicalType>& inputTypes) {
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

uint32_t BuiltInVectorOperations::getCastCost(
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

uint32_t BuiltInVectorOperations::getCastCost(
    const LogicalType& inputType, const LogicalType& targetType) {
    if (inputType == targetType) {
        return 0;
    } else {
        switch (inputType.getLogicalTypeID()) {
        case common::LogicalTypeID::FIXED_LIST:
        case common::LogicalTypeID::VAR_LIST:
        case common::LogicalTypeID::MAP:
        case common::LogicalTypeID::UNION:
        case common::LogicalTypeID::STRUCT:
        case common::LogicalTypeID::INTERNAL_ID:
        // TODO(Ziyi): add boolean cast operations.
        case common::LogicalTypeID::BOOL:
            return UNDEFINED_CAST_COST;
        default:
            return getCastCost(inputType.getLogicalTypeID(), targetType.getLogicalTypeID());
        }
    }
}

uint32_t BuiltInVectorOperations::getTargetTypeCost(common::LogicalTypeID typeID) {
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

uint32_t BuiltInVectorOperations::castInt64(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::FLOAT:
    case common::LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorOperations::castInt32(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::INT64:
    case common::LogicalTypeID::FLOAT:
    case common::LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorOperations::castInt16(common::LogicalTypeID targetTypeID) {
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

uint32_t BuiltInVectorOperations::castDouble(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorOperations::castFloat(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorOperations::castDate(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::TIMESTAMP:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInVectorOperations::castSerial(common::LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case common::LogicalTypeID::INT64:
        return 0;
    default:
        return castInt64(targetTypeID);
    }
}

// When there is multiple candidates functions, e.g. double + int and double + double for input
// "1.5 + parameter", we prefer the one without any implicit casting i.e. double + double.
VectorOperationDefinition* BuiltInVectorOperations::getBestMatch(
    std::vector<VectorOperationDefinition*>& functions) {
    assert(functions.size() > 1);
    VectorOperationDefinition* result = nullptr;
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

uint32_t BuiltInVectorOperations::getFunctionCost(const std::vector<LogicalType>& inputTypes,
    VectorOperationDefinition* function, bool isOverload) {
    if (function->isVarLength) {
        assert(function->parameterTypeIDs.size() == 1);
        return matchVarLengthParameters(inputTypes, function->parameterTypeIDs[0], isOverload);
    } else {
        return matchParameters(inputTypes, function->parameterTypeIDs, isOverload);
    }
}

uint32_t BuiltInVectorOperations::matchParameters(const std::vector<LogicalType>& inputTypes,
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

uint32_t BuiltInVectorOperations::matchVarLengthParameters(
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

void BuiltInVectorOperations::validateNonEmptyCandidateFunctions(
    std::vector<VectorOperationDefinition*>& candidateFunctions, const std::string& name,
    const std::vector<LogicalType>& inputTypes) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& functionDefinition : vectorOperations.at(name)) {
            supportedInputsString += functionDefinition->signatureToString() + "\n";
        }
        throw BinderException("Cannot match a built-in function for given function " + name +
                              LogicalTypeUtils::dataTypesToString(inputTypes) +
                              ". Supported inputs are\n" + supportedInputsString);
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

void BuiltInVectorOperations::registerBlobOperations() {
    vectorOperations.insert(
        {OCTET_LENGTH_FUNC_NAME, OctetLengthVectorOperations::getDefinitions()});
    vectorOperations.insert({ENCODE_FUNC_NAME, EncodeVectorOperations::getDefinitions()});
    vectorOperations.insert({DECODE_FUNC_NAME, DecodeVectorOperations::getDefinitions()});
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
    vectorOperations.insert(
        {REGEXP_FULL_MATCH_FUNC_NAME, RegexpFullMatchVectorOperation::getDefinitions()});
    vectorOperations.insert({REGEXP_MATCHES_FUNC_NAME, RegexpMatchesOperation::getDefinitions()});
    vectorOperations.insert({REGEXP_REPLACE_FUNC_NAME, RegexpReplaceOperation::getDefinitions()});
    vectorOperations.insert({REGEXP_EXTRACT_FUNC_NAME, RegexpExtractOperation::getDefinitions()});
    vectorOperations.insert(
        {REGEXP_EXTRACT_ALL_FUNC_NAME, RegexpExtractAllOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerCastOperations() {
    vectorOperations.insert({CAST_TO_DATE_FUNC_NAME, CastToDateVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_TIMESTAMP_FUNC_NAME, CastToTimestampVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_INTERVAL_FUNC_NAME, CastToIntervalVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_STRING_FUNC_NAME, CastToStringVectorOperation::getDefinitions()});
    vectorOperations.insert({CAST_TO_BLOB_FUNC_NAME, CastToBlobVectorOperation::getDefinitions()});
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
    vectorOperations.insert({LIST_SORT_FUNC_NAME, ListSortVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {LIST_REVERSE_SORT_FUNC_NAME, ListReverseSortVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_SUM_FUNC_NAME, ListSumVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {LIST_DISTINCT_FUNC_NAME, ListDistinctVectorOperation::getDefinitions()});
    vectorOperations.insert({LIST_UNIQUE_FUNC_NAME, ListUniqueVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {LIST_ANY_VALUE_FUNC_NAME, ListAnyValueVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerStructOperations() {
    vectorOperations.insert({STRUCT_PACK_FUNC_NAME, StructPackVectorOperations::getDefinitions()});
    vectorOperations.insert(
        {STRUCT_EXTRACT_FUNC_NAME, StructExtractVectorOperations::getDefinitions()});
}

void BuiltInVectorOperations::registerMapOperations() {
    vectorOperations.insert(
        {MAP_CREATION_FUNC_NAME, MapCreationVectorOperations::getDefinitions()});
    vectorOperations.insert({MAP_EXTRACT_FUNC_NAME, MapExtractVectorOperations::getDefinitions()});
    vectorOperations.insert({ELEMENT_AT_FUNC_NAME, MapExtractVectorOperations::getDefinitions()});
    vectorOperations.insert({CARDINALITY_FUNC_NAME, ListLenVectorOperation::getDefinitions()});
    vectorOperations.insert({MAP_KEYS_FUNC_NAME, MapKeysVectorOperations::getDefinitions()});
    vectorOperations.insert({MAP_VALUES_FUNC_NAME, MapValuesVectorOperations::getDefinitions()});
}

void BuiltInVectorOperations::registerUnionOperations() {
    vectorOperations.insert({UNION_VALUE_FUNC_NAME, UnionValueVectorOperations::getDefinitions()});
    vectorOperations.insert({UNION_TAG_FUNC_NAME, UnionTagVectorOperations::getDefinitions()});
    vectorOperations.insert(
        {UNION_EXTRACT_FUNC_NAME, UnionExtractVectorOperations::getDefinitions()});
}

void BuiltInVectorOperations::registerNodeRelOperations() {
    vectorOperations.insert({OFFSET_FUNC_NAME, OffsetVectorOperation::getDefinitions()});
    vectorOperations.insert({NODES_FUNC_NAME, NodesVectorOperation::getDefinitions()});
    vectorOperations.insert({RELS_FUNC_NAME, RelsVectorOperation::getDefinitions()});
}

} // namespace function
} // namespace kuzu
