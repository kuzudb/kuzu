#include "common/exception/binder.h"
#include "common/exception/catalog.h"
#include "common/string_format.h"
#include "function/aggregate/collect.h"
#include "function/aggregate/count.h"
#include "function/aggregate/count_star.h"
#include "function/aggregate_function.h"
#include "function/arithmetic/vector_arithmetic_functions.h"
#include "function/blob/vector_blob_functions.h"
#include "function/built_in_function.h"
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
#include "function/table_functions/call_functions.h"
#include "function/timestamp/vector_timestamp_functions.h"
#include "function/union/vector_union_functions.h"
#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"
#include "processor/operator/persistent/reader/npy/npy_reader.h"
#include "processor/operator/persistent/reader/parquet/parquet_reader.h"
#include "processor/operator/persistent/reader/rdf/rdf_reader.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

BuiltInFunctions::BuiltInFunctions() {
    registerScalarFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
}

void BuiltInFunctions::registerScalarFunctions() {
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

void BuiltInFunctions::registerAggregateFunctions() {
    registerCountStar();
    registerCount();
    registerSum();
    registerAvg();
    registerMin();
    registerMax();
    registerCollect();
}

Function* BuiltInFunctions::matchScalarFunction(
    const std::string& name, const std::vector<LogicalType*>& inputTypes) {
    auto& functionSet = functions.at(name);
    bool isOverload = functionSet.size() > 1;
    std::vector<Function*> candidateFunctions;
    uint32_t minCost = UINT32_MAX;
    for (auto& function : functionSet) {
        auto func = reinterpret_cast<Function*>(function.get());
        if (name == CAST_FUNC_NAME) {
            return func;
        }
        auto cost = getFunctionCost(inputTypes, func, isOverload);
        if (cost == UINT32_MAX) {
            continue;
        }
        if (cost < minCost) {
            candidateFunctions.clear();
            candidateFunctions.push_back(func);
            minCost = cost;
        } else if (cost == minCost) {
            candidateFunctions.push_back(func);
        }
    }
    validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes);
    if (candidateFunctions.size() > 1) {
        return getBestMatch(candidateFunctions);
    }
    return candidateFunctions[0];
}

AggregateFunction* BuiltInFunctions::matchAggregateFunction(
    const std::string& name, const std::vector<common::LogicalType*>& inputTypes, bool isDistinct) {
    auto& functionSet = functions.at(name);
    std::vector<AggregateFunction*> candidateFunctions;
    for (auto& function : functionSet) {
        auto aggregateFunction = reinterpret_cast<AggregateFunction*>(function.get());
        auto cost = getAggregateFunctionCost(inputTypes, isDistinct, aggregateFunction);
        if (cost == UINT32_MAX) {
            continue;
        }
        candidateFunctions.push_back(aggregateFunction);
    }
    validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes, isDistinct);
    KU_ASSERT(candidateFunctions.size() == 1);
    return candidateFunctions[0];
}

uint32_t BuiltInFunctions::getCastCost(LogicalTypeID inputTypeID, LogicalTypeID targetTypeID) {
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
        case LogicalTypeID::RDF_VARIANT:
            // RDF_VARIANT can be cast to any type.
            return 1;
        case LogicalTypeID::INT64:
            return castInt64(targetTypeID);
        case LogicalTypeID::INT32:
            return castInt32(targetTypeID);
        case LogicalTypeID::INT16:
            return castInt16(targetTypeID);
        case LogicalTypeID::INT8:
            return castInt8(targetTypeID);
        case LogicalTypeID::UINT64:
            return castUInt64(targetTypeID);
        case LogicalTypeID::UINT32:
            return castUInt32(targetTypeID);
        case LogicalTypeID::UINT16:
            return castUInt16(targetTypeID);
        case LogicalTypeID::UINT8:
            return castUInt8(targetTypeID);
        case LogicalTypeID::INT128:
            return castInt128(targetTypeID);
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

uint32_t BuiltInFunctions::getAggregateFunctionCost(
    const std::vector<LogicalType*>& inputTypes, bool isDistinct, AggregateFunction* function) {
    if (inputTypes.size() != function->parameterTypeIDs.size() ||
        isDistinct != function->isDistinct) {
        return UINT32_MAX;
    }
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        if (function->parameterTypeIDs[i] == LogicalTypeID::ANY) {
            continue;
        } else if (inputTypes[i]->getLogicalTypeID() != function->parameterTypeIDs[i]) {
            return UINT32_MAX;
        }
    }
    return 0;
}

void BuiltInFunctions::validateNonEmptyCandidateFunctions(
    std::vector<AggregateFunction*>& candidateFunctions, const std::string& name,
    const std::vector<LogicalType*>& inputTypes, bool isDistinct) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& function : functions.at(name)) {
            auto aggregateFunction = reinterpret_cast<AggregateFunction*>(function.get());
            if (aggregateFunction->isDistinct) {
                supportedInputsString += "DISTINCT ";
            }
            supportedInputsString += aggregateFunction->signatureToString() + "\n";
        }
        throw BinderException("Cannot match a built-in function for given function " + name +
                              (isDistinct ? "DISTINCT " : "") +
                              LogicalTypeUtils::toString(inputTypes) + ". Supported inputs are\n" +
                              supportedInputsString);
    }
}

std::unique_ptr<BuiltInFunctions> BuiltInFunctions::copy() {
    auto result = std::make_unique<BuiltInFunctions>();
    for (auto& [name, functionSet] : functions) {
        std::vector<std::unique_ptr<Function>> functionSetToCopy;
        for (auto& function : functionSet) {
            functionSetToCopy.push_back(function->copy());
        }
        result->functions.emplace(name, std::move(functionSetToCopy));
    }
    return result;
}

uint32_t BuiltInFunctions::getTargetTypeCost(LogicalTypeID typeID) {
    switch (typeID) {
    case LogicalTypeID::INT16:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16: {
        return 110;
    }
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
    case LogicalTypeID::INT128:
    case LogicalTypeID::TIMESTAMP: {
        return 120;
    }
        // LCOV_EXCL_START
    default: {
        throw InternalException("Unsupported casting operation.");
        // LCOC_EXCL_STOP
    }
    }
}

uint32_t BuiltInFunctions::castInt64(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castInt32(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castInt16(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castInt8(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castUInt64(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castUInt32(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castUInt16(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castUInt8(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castInt128(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castDouble(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castFloat(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castDate(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::TIMESTAMP:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctions::castSerial(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT64:
        return 0;
    default:
        return castInt64(targetTypeID);
    }
}

// When there is multiple candidates functions, e.g. double + int and double + double for input
// "1.5 + parameter", we prefer the one without any implicit casting i.e. double + double.
// Additionally, we prefer function with string parameter because string is most permissive and can
// be cast to any type.
Function* BuiltInFunctions::getBestMatch(std::vector<Function*>& functionsToMatch) {
    KU_ASSERT(functionsToMatch.size() > 1);
    Function* result = nullptr;
    auto cost = UNDEFINED_CAST_COST;
    for (auto& function : functionsToMatch) {
        auto currentCost = 0;
        std::unordered_set<LogicalTypeID> distinctParameterTypes;
        for (auto& parameterTypeID : function->parameterTypeIDs) {
            if (parameterTypeID != LogicalTypeID::STRING) {
                currentCost++;
            }
            if (!distinctParameterTypes.contains(parameterTypeID)) {
                currentCost++;
                distinctParameterTypes.insert(parameterTypeID);
            }
        }
        if (currentCost < cost) {
            cost = currentCost;
            result = function;
        }
    }
    KU_ASSERT(result != nullptr);
    return result;
}

uint32_t BuiltInFunctions::getFunctionCost(
    const std::vector<LogicalType*>& inputTypes, Function* function, bool isOverload) {
    switch (function->type) {
    case FunctionType::SCALAR: {
        auto scalarFunction = reinterpret_cast<ScalarFunction*>(function);
        if (scalarFunction->isVarLength) {
            KU_ASSERT(function->parameterTypeIDs.size() == 1);
            return matchVarLengthParameters(inputTypes, function->parameterTypeIDs[0], isOverload);
        } else {
            return matchParameters(inputTypes, function->parameterTypeIDs, isOverload);
        }
    }
    case FunctionType::TABLE:
        return matchParameters(inputTypes, function->parameterTypeIDs, isOverload);
    default:
        KU_UNREACHABLE;
    }
}

uint32_t BuiltInFunctions::matchParameters(const std::vector<LogicalType*>& inputTypes,
    const std::vector<LogicalTypeID>& targetTypeIDs, bool /*isOverload*/) {
    if (inputTypes.size() != targetTypeIDs.size()) {
        return UINT32_MAX;
    }
    auto cost = 0u;
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        auto castCost = getCastCost(inputTypes[i]->getLogicalTypeID(), targetTypeIDs[i]);
        if (castCost == UNDEFINED_CAST_COST) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

uint32_t BuiltInFunctions::matchVarLengthParameters(
    const std::vector<LogicalType*>& inputTypes, LogicalTypeID targetTypeID, bool /*isOverload*/) {
    auto cost = 0u;
    for (auto inputType : inputTypes) {
        auto castCost = getCastCost(inputType->getLogicalTypeID(), targetTypeID);
        if (castCost == UNDEFINED_CAST_COST) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

void BuiltInFunctions::validateNonEmptyCandidateFunctions(
    std::vector<Function*>& candidateFunctions, const std::string& name,
    const std::vector<LogicalType*>& inputTypes) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& function : functions.at(name)) {
            auto baseScalarFunction = reinterpret_cast<BaseScalarFunction*>(function.get());
            supportedInputsString += baseScalarFunction->signatureToString() + "\n";
        }
        throw BinderException("Cannot match a built-in function for given function " + name +
                              LogicalTypeUtils::toString(inputTypes) + ". Supported inputs are\n" +
                              supportedInputsString);
    }
}

void BuiltInFunctions::registerComparisonFunctions() {
    functions.insert({EQUALS_FUNC_NAME, EqualsFunction::getFunctionSet()});
    functions.insert({NOT_EQUALS_FUNC_NAME, NotEqualsFunction::getFunctionSet()});
    functions.insert({GREATER_THAN_FUNC_NAME, GreaterThanFunction::getFunctionSet()});
    functions.insert({GREATER_THAN_EQUALS_FUNC_NAME, GreaterThanEqualsFunction::getFunctionSet()});
    functions.insert({LESS_THAN_FUNC_NAME, LessThanFunction::getFunctionSet()});
    functions.insert({LESS_THAN_EQUALS_FUNC_NAME, LessThanEqualsFunction::getFunctionSet()});
}

void BuiltInFunctions::registerArithmeticFunctions() {
    functions.insert({ADD_FUNC_NAME, AddFunction::getFunctionSet()});
    functions.insert({SUBTRACT_FUNC_NAME, SubtractFunction::getFunctionSet()});
    functions.insert({MULTIPLY_FUNC_NAME, MultiplyFunction::getFunctionSet()});
    functions.insert({DIVIDE_FUNC_NAME, DivideFunction::getFunctionSet()});
    functions.insert({MODULO_FUNC_NAME, ModuloFunction::getFunctionSet()});
    functions.insert({POWER_FUNC_NAME, PowerFunction::getFunctionSet()});

    functions.insert({ABS_FUNC_NAME, AbsFunction::getFunctionSet()});
    functions.insert({ACOS_FUNC_NAME, AcosFunction::getFunctionSet()});
    functions.insert({ASIN_FUNC_NAME, AsinFunction::getFunctionSet()});
    functions.insert({ATAN_FUNC_NAME, AtanFunction::getFunctionSet()});
    functions.insert({ATAN2_FUNC_NAME, Atan2Function::getFunctionSet()});
    functions.insert({BITWISE_XOR_FUNC_NAME, BitwiseXorFunction::getFunctionSet()});
    functions.insert({BITWISE_AND_FUNC_NAME, BitwiseAndFunction::getFunctionSet()});
    functions.insert({BITWISE_OR_FUNC_NAME, BitwiseOrFunction::getFunctionSet()});
    functions.insert({BITSHIFT_LEFT_FUNC_NAME, BitShiftLeftFunction::getFunctionSet()});
    functions.insert({BITSHIFT_RIGHT_FUNC_NAME, BitShiftRightFunction::getFunctionSet()});
    functions.insert({CBRT_FUNC_NAME, CbrtFunction::getFunctionSet()});
    functions.insert({CEIL_FUNC_NAME, CeilFunction::getFunctionSet()});
    functions.insert({CEILING_FUNC_NAME, CeilFunction::getFunctionSet()});
    functions.insert({COS_FUNC_NAME, CosFunction::getFunctionSet()});
    functions.insert({COT_FUNC_NAME, CotFunction::getFunctionSet()});
    functions.insert({DEGREES_FUNC_NAME, DegreesFunction::getFunctionSet()});
    functions.insert({EVEN_FUNC_NAME, EvenFunction::getFunctionSet()});
    functions.insert({FACTORIAL_FUNC_NAME, FactorialFunction::getFunctionSet()});
    functions.insert({FLOOR_FUNC_NAME, FloorFunction::getFunctionSet()});
    functions.insert({GAMMA_FUNC_NAME, GammaFunction::getFunctionSet()});
    functions.insert({LGAMMA_FUNC_NAME, LgammaFunction::getFunctionSet()});
    functions.insert({LN_FUNC_NAME, LnFunction::getFunctionSet()});
    functions.insert({LOG_FUNC_NAME, LogFunction::getFunctionSet()});
    functions.insert({LOG2_FUNC_NAME, Log2Function::getFunctionSet()});
    functions.insert({LOG10_FUNC_NAME, LogFunction::getFunctionSet()});
    functions.insert({NEGATE_FUNC_NAME, NegateFunction::getFunctionSet()});
    functions.insert({PI_FUNC_NAME, PiFunction::getFunctionSet()});
    functions.insert({POW_FUNC_NAME, PowerFunction::getFunctionSet()});
    functions.insert({RADIANS_FUNC_NAME, RadiansFunction::getFunctionSet()});
    functions.insert({ROUND_FUNC_NAME, RoundFunction::getFunctionSet()});
    functions.insert({SIN_FUNC_NAME, SinFunction::getFunctionSet()});
    functions.insert({SIGN_FUNC_NAME, SignFunction::getFunctionSet()});
    functions.insert({SQRT_FUNC_NAME, SqrtFunction::getFunctionSet()});
    functions.insert({TAN_FUNC_NAME, TanFunction::getFunctionSet()});
}

void BuiltInFunctions::registerDateFunctions() {
    functions.insert({DATE_PART_FUNC_NAME, DatePartFunction::getFunctionSet()});
    functions.insert({DATEPART_FUNC_NAME, DatePartFunction::getFunctionSet()});
    functions.insert({DATE_TRUNC_FUNC_NAME, DateTruncFunction::getFunctionSet()});
    functions.insert({DATETRUNC_FUNC_NAME, DateTruncFunction::getFunctionSet()});
    functions.insert({DAYNAME_FUNC_NAME, DayNameFunction::getFunctionSet()});
    functions.insert({GREATEST_FUNC_NAME, GreatestFunction::getFunctionSet()});
    functions.insert({LAST_DAY_FUNC_NAME, LastDayFunction::getFunctionSet()});
    functions.insert({LEAST_FUNC_NAME, LeastFunction::getFunctionSet()});
    functions.insert({MAKE_DATE_FUNC_NAME, MakeDateFunction::getFunctionSet()});
    functions.insert({MONTHNAME_FUNC_NAME, MonthNameFunction::getFunctionSet()});
}

void BuiltInFunctions::registerTimestampFunctions() {
    functions.insert({CENTURY_FUNC_NAME, CenturyFunction::getFunctionSet()});
    functions.insert({EPOCH_MS_FUNC_NAME, EpochMsFunction::getFunctionSet()});
    functions.insert({TO_TIMESTAMP_FUNC_NAME, ToTimestampFunction::getFunctionSet()});
}

void BuiltInFunctions::registerIntervalFunctions() {
    functions.insert({TO_YEARS_FUNC_NAME, ToYearsFunction::getFunctionSet()});
    functions.insert({TO_MONTHS_FUNC_NAME, ToMonthsFunction::getFunctionSet()});
    functions.insert({TO_DAYS_FUNC_NAME, ToDaysFunction::getFunctionSet()});
    functions.insert({TO_HOURS_FUNC_NAME, ToHoursFunction::getFunctionSet()});
    functions.insert({TO_MINUTES_FUNC_NAME, ToMinutesFunction::getFunctionSet()});
    functions.insert({TO_SECONDS_FUNC_NAME, ToSecondsFunction::getFunctionSet()});
    functions.insert({TO_MILLISECONDS_FUNC_NAME, ToMillisecondsFunction::getFunctionSet()});
    functions.insert({TO_MICROSECONDS_FUNC_NAME, ToMicrosecondsFunction::getFunctionSet()});
}

void BuiltInFunctions::registerBlobFunctions() {
    functions.insert({OCTET_LENGTH_FUNC_NAME, OctetLengthFunctions::getFunctionSet()});
    functions.insert({ENCODE_FUNC_NAME, EncodeFunctions::getFunctionSet()});
    functions.insert({DECODE_FUNC_NAME, DecodeFunctions::getFunctionSet()});
}

void BuiltInFunctions::registerStringFunctions() {
    functions.insert({ARRAY_EXTRACT_FUNC_NAME, ArrayExtractFunction::getFunctionSet()});
    functions.insert({CONCAT_FUNC_NAME, ConcatFunction::getFunctionSet()});
    functions.insert({CONTAINS_FUNC_NAME, ContainsFunction::getFunctionSet()});
    functions.insert({ENDS_WITH_FUNC_NAME, EndsWithFunction::getFunctionSet()});
    functions.insert({LCASE_FUNC_NAME, LowerFunction::getFunctionSet()});
    functions.insert({LEFT_FUNC_NAME, LeftFunction::getFunctionSet()});
    functions.insert({LOWER_FUNC_NAME, LowerFunction::getFunctionSet()});
    functions.insert({LPAD_FUNC_NAME, LpadFunction::getFunctionSet()});
    functions.insert({LTRIM_FUNC_NAME, LtrimFunction::getFunctionSet()});
    functions.insert({PREFIX_FUNC_NAME, StartsWithFunction::getFunctionSet()});
    functions.insert({REPEAT_FUNC_NAME, RepeatFunction::getFunctionSet()});
    functions.insert({REVERSE_FUNC_NAME, ReverseFunction::getFunctionSet()});
    functions.insert({RIGHT_FUNC_NAME, RightFunction::getFunctionSet()});
    functions.insert({RPAD_FUNC_NAME, RpadFunction::getFunctionSet()});
    functions.insert({RTRIM_FUNC_NAME, RtrimFunction::getFunctionSet()});
    functions.insert({STARTS_WITH_FUNC_NAME, StartsWithFunction::getFunctionSet()});
    functions.insert({SUBSTR_FUNC_NAME, SubStrFunction::getFunctionSet()});
    functions.insert({SUBSTRING_FUNC_NAME, SubStrFunction::getFunctionSet()});
    functions.insert({SUFFIX_FUNC_NAME, EndsWithFunction::getFunctionSet()});
    functions.insert({TRIM_FUNC_NAME, TrimFunction::getFunctionSet()});
    functions.insert({UCASE_FUNC_NAME, UpperFunction::getFunctionSet()});
    functions.insert({UPPER_FUNC_NAME, UpperFunction::getFunctionSet()});
    functions.insert({REGEXP_FULL_MATCH_FUNC_NAME, RegexpFullMatchFunction::getFunctionSet()});
    functions.insert({REGEXP_MATCHES_FUNC_NAME, RegexpMatchesFunction::getFunctionSet()});
    functions.insert({REGEXP_REPLACE_FUNC_NAME, RegexpReplaceFunction::getFunctionSet()});
    functions.insert({REGEXP_EXTRACT_FUNC_NAME, RegexpExtractFunction::getFunctionSet()});
    functions.insert({REGEXP_EXTRACT_ALL_FUNC_NAME, RegexpExtractAllFunction::getFunctionSet()});
}

void BuiltInFunctions::registerCastFunctions() {
    functions.insert({CAST_DATE_FUNC_NAME, CastToDateFunction::getFunctionSet()});
    functions.insert({CAST_TO_DATE_FUNC_NAME, CastToDateFunction::getFunctionSet()});
    functions.insert({CAST_TO_TIMESTAMP_FUNC_NAME, CastToTimestampFunction::getFunctionSet()});
    functions.insert({CAST_INTERVAL_FUNC_NAME, CastToIntervalFunction::getFunctionSet()});
    functions.insert({CAST_TO_INTERVAL_FUNC_NAME, CastToIntervalFunction::getFunctionSet()});
    functions.insert({CAST_STRING_FUNC_NAME, CastToStringFunction::getFunctionSet()});
    functions.insert({CAST_TO_STRING_FUNC_NAME, CastToStringFunction::getFunctionSet()});
    functions.insert({CAST_BLOB_FUNC_NAME, CastToBlobFunction::getFunctionSet()});
    functions.insert({CAST_TO_BLOB_FUNC_NAME, CastToBlobFunction::getFunctionSet()});
    functions.insert({CAST_TO_DOUBLE_FUNC_NAME, CastToDoubleFunction::getFunctionSet()});
    functions.insert({CAST_TO_FLOAT_FUNC_NAME, CastToFloatFunction::getFunctionSet()});
    functions.insert({CAST_TO_SERIAL_FUNC_NAME, CastToSerialFunction::getFunctionSet()});
    functions.insert({CAST_TO_INT64_FUNC_NAME, CastToInt64Function::getFunctionSet()});
    functions.insert({CAST_TO_INT32_FUNC_NAME, CastToInt32Function::getFunctionSet()});
    functions.insert({CAST_TO_INT16_FUNC_NAME, CastToInt16Function::getFunctionSet()});
    functions.insert({CAST_TO_INT8_FUNC_NAME, CastToInt8Function::getFunctionSet()});
    functions.insert({CAST_TO_UINT64_FUNC_NAME, CastToUInt64Function::getFunctionSet()});
    functions.insert({CAST_TO_UINT32_FUNC_NAME, CastToUInt32Function::getFunctionSet()});
    functions.insert({CAST_TO_UINT16_FUNC_NAME, CastToUInt16Function::getFunctionSet()});
    functions.insert({CAST_TO_UINT8_FUNC_NAME, CastToUInt8Function::getFunctionSet()});
    functions.insert({CAST_TO_INT128_FUNC_NAME, CastToInt128Function::getFunctionSet()});
    functions.insert({CAST_TO_BOOL_FUNC_NAME, CastToBoolFunction::getFunctionSet()});
    functions.insert({CAST_FUNC_NAME, CastAnyFunction::getFunctionSet()});
}

void BuiltInFunctions::registerListFunctions() {
    functions.insert({LIST_CREATION_FUNC_NAME, ListCreationFunction::getFunctionSet()});
    functions.insert({LIST_RANGE_FUNC_NAME, ListRangeFunction::getFunctionSet()});
    functions.insert({SIZE_FUNC_NAME, SizeFunction::getFunctionSet()});
    functions.insert({LIST_EXTRACT_FUNC_NAME, ListExtractFunction::getFunctionSet()});
    functions.insert({LIST_ELEMENT_FUNC_NAME, ListExtractFunction::getFunctionSet()});
    functions.insert({LIST_CONCAT_FUNC_NAME, ListConcatFunction::getFunctionSet()});
    functions.insert({LIST_CAT_FUNC_NAME, ListConcatFunction::getFunctionSet()});
    functions.insert({ARRAY_CONCAT_FUNC_NAME, ListConcatFunction::getFunctionSet()});
    functions.insert({ARRAY_CAT_FUNC_NAME, ListConcatFunction::getFunctionSet()});
    functions.insert({LIST_APPEND_FUNC_NAME, ListAppendFunction::getFunctionSet()});
    functions.insert({ARRAY_APPEND_FUNC_NAME, ListAppendFunction::getFunctionSet()});
    functions.insert({ARRAY_PUSH_BACK_FUNC_NAME, ListAppendFunction::getFunctionSet()});
    functions.insert({LIST_PREPEND_FUNC_NAME, ListPrependFunction::getFunctionSet()});
    functions.insert({ARRAY_PREPEND_FUNC_NAME, ListPrependFunction::getFunctionSet()});
    functions.insert({ARRAY_PUSH_FRONT_FUNC_NAME, ListPrependFunction::getFunctionSet()});
    functions.insert({LIST_POSITION_FUNC_NAME, ListPositionFunction::getFunctionSet()});
    functions.insert({ARRAY_POSITION_FUNC_NAME, ListPositionFunction::getFunctionSet()});
    functions.insert({LIST_INDEXOF_FUNC_NAME, ListPositionFunction::getFunctionSet()});
    functions.insert({ARRAY_INDEXOF_FUNC_NAME, ListPositionFunction::getFunctionSet()});
    functions.insert({LIST_CONTAINS_FUNC_NAME, ListContainsFunction::getFunctionSet()});
    functions.insert({LIST_HAS_FUNC_NAME, ListContainsFunction::getFunctionSet()});
    functions.insert({ARRAY_CONTAINS_FUNC_NAME, ListContainsFunction::getFunctionSet()});
    functions.insert({ARRAY_HAS_FUNC_NAME, ListContainsFunction::getFunctionSet()});
    functions.insert({LIST_SLICE_FUNC_NAME, ListSliceFunction::getFunctionSet()});
    functions.insert({ARRAY_SLICE_FUNC_NAME, ListSliceFunction::getFunctionSet()});
    functions.insert({LIST_SORT_FUNC_NAME, ListSortFunction::getFunctionSet()});
    functions.insert({LIST_REVERSE_SORT_FUNC_NAME, ListReverseSortFunction::getFunctionSet()});
    functions.insert({LIST_SUM_FUNC_NAME, ListSumFunction::getFunctionSet()});
    functions.insert({LIST_PRODUCT_FUNC_NAME, ListProductFunction::getFunctionSet()});
    functions.insert({LIST_DISTINCT_FUNC_NAME, ListDistinctFunction::getFunctionSet()});
    functions.insert({LIST_UNIQUE_FUNC_NAME, ListUniqueFunction::getFunctionSet()});
    functions.insert({LIST_ANY_VALUE_FUNC_NAME, ListAnyValueFunction::getFunctionSet()});
}

void BuiltInFunctions::registerStructFunctions() {
    functions.insert({STRUCT_PACK_FUNC_NAME, StructPackFunctions::getFunctionSet()});
    functions.insert({STRUCT_EXTRACT_FUNC_NAME, StructExtractFunctions::getFunctionSet()});
}

void BuiltInFunctions::registerMapFunctions() {
    functions.insert({MAP_CREATION_FUNC_NAME, MapCreationFunctions::getFunctionSet()});
    functions.insert({MAP_EXTRACT_FUNC_NAME, MapExtractFunctions::getFunctionSet()});
    functions.insert({ELEMENT_AT_FUNC_NAME, MapExtractFunctions::getFunctionSet()});
    functions.insert({CARDINALITY_FUNC_NAME, SizeFunction::getFunctionSet()});
    functions.insert({MAP_KEYS_FUNC_NAME, MapKeysFunctions::getFunctionSet()});
    functions.insert({MAP_VALUES_FUNC_NAME, MapValuesFunctions::getFunctionSet()});
}

void BuiltInFunctions::registerUnionFunctions() {
    functions.insert({UNION_VALUE_FUNC_NAME, UnionValueFunction::getFunctionSet()});
    functions.insert({UNION_TAG_FUNC_NAME, UnionTagFunction::getFunctionSet()});
    functions.insert({UNION_EXTRACT_FUNC_NAME, UnionExtractFunction::getFunctionSet()});
}

void BuiltInFunctions::registerNodeRelFunctions() {
    functions.insert({OFFSET_FUNC_NAME, OffsetFunction::getFunctionSet()});
}

void BuiltInFunctions::registerPathFunctions() {
    functions.insert({NODES_FUNC_NAME, NodesFunction::getFunctionSet()});
    functions.insert({RELS_FUNC_NAME, RelsFunction::getFunctionSet()});
    functions.insert({PROPERTIES_FUNC_NAME, PropertiesFunction::getFunctionSet()});
    functions.insert({IS_TRAIL_FUNC_NAME, IsTrailFunction::getFunctionSet()});
    functions.insert({IS_ACYCLIC_FUNC_NAME, IsACyclicFunction::getFunctionSet()});
}

void BuiltInFunctions::registerCountStar() {
    function_set functionSet;
    functionSet.push_back(std::make_unique<AggregateFunction>(COUNT_STAR_FUNC_NAME,
        std::vector<common::LogicalTypeID>{}, LogicalTypeID::INT64, CountStarFunction::initialize,
        CountStarFunction::updateAll, CountStarFunction::updatePos, CountStarFunction::combine,
        CountStarFunction::finalize, false));
    functions.insert({COUNT_STAR_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerCount() {
    function_set functionSet;
    for (auto& type : LogicalTypeUtils::getAllValidLogicTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getAggFunc<CountFunction>(COUNT_FUNC_NAME,
                type.getLogicalTypeID(), LogicalTypeID::INT64, isDistinct,
                CountFunction::paramRewriteFunc));
        }
    }
    functions.insert({COUNT_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerSum() {
    function_set functionSet;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(
                AggregateFunctionUtil::getSumFunc(SUM_FUNC_NAME, typeID, typeID, isDistinct));
        }
    }
    functions.insert({SUM_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerAvg() {
    function_set functionSet;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getAvgFunc(
                AVG_FUNC_NAME, typeID, LogicalTypeID::DOUBLE, isDistinct));
        }
    }
    functions.insert({AVG_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerMin() {
    function_set functionSet;
    for (auto& type : LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getMinFunc(type, isDistinct));
        }
    }
    functions.insert({MIN_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerMax() {
    function_set functionSet;
    for (auto& type : LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getMaxFunc(type, isDistinct));
        }
    }
    functions.insert({MAX_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerCollect() {
    function_set functionSet;
    for (auto isDistinct : std::vector<bool>{true, false}) {
        functionSet.push_back(std::make_unique<AggregateFunction>(COLLECT_FUNC_NAME,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::ANY}, LogicalTypeID::VAR_LIST,
            CollectFunction::initialize, CollectFunction::updateAll, CollectFunction::updatePos,
            CollectFunction::combine, CollectFunction::finalize, isDistinct,
            CollectFunction::bindFunc));
    }
    functions.insert({COLLECT_FUNC_NAME, std::move(functionSet)});
}

void BuiltInFunctions::registerTableFunctions() {
    functions.insert({CURRENT_SETTING_FUNC_NAME, CurrentSettingFunction::getFunctionSet()});
    functions.insert({DB_VERSION_FUNC_NAME, DBVersionFunction::getFunctionSet()});
    functions.insert({SHOW_TABLES_FUNC_NAME, ShowTablesFunction::getFunctionSet()});
    functions.insert({TABLE_INFO_FUNC_NAME, TableInfoFunction::getFunctionSet()});
    functions.insert({SHOW_CONNECTION_FUNC_NAME, ShowConnectionFunction::getFunctionSet()});
    functions.insert({READ_PARQUET_FUNC_NAME, processor::ParquetScanFunction::getFunctionSet()});
    functions.insert({READ_NPY_FUNC_NAME, processor::NpyScanFunction::getFunctionSet()});
    functions.insert({READ_CSV_SERIAL_FUNC_NAME, processor::SerialCSVScan::getFunctionSet()});
    functions.insert({READ_CSV_PARALLEL_FUNC_NAME, processor::ParallelCSVScan::getFunctionSet()});
    functions.insert({READ_RDF_FUNC_NAME, processor::RdfScan::getFunctionSet()});
}

void BuiltInFunctions::addFunction(std::string name, function::function_set definitions) {
    if (functions.contains(name)) {
        throw CatalogException{stringFormat("function {} already exists.", name)};
    }
    functions.emplace(std::move(name), std::move(definitions));
}

} // namespace function
} // namespace kuzu
