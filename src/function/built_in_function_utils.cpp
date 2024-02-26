#include "function/built_in_function_utils.h"

#include "catalog/catalog_entry/aggregate_function_catalog_entry.h"
#include "catalog/catalog_entry/scalar_function_catalog_entry.h"
#include "catalog/catalog_entry/table_function_catalog_entry.h"
#include "catalog/catalog_set.h"
#include "common/exception/binder.h"
#include "common/exception/catalog.h"
#include "function/aggregate/collect.h"
#include "function/aggregate/count.h"
#include "function/aggregate/count_star.h"
#include "function/aggregate_function.h"
#include "function/arithmetic/vector_arithmetic_functions.h"
#include "function/blob/vector_blob_functions.h"
#include "function/cast/vector_cast_functions.h"
#include "function/comparison/vector_comparison_functions.h"
#include "function/date/vector_date_functions.h"
#include "function/interval/vector_interval_functions.h"
#include "function/list/vector_list_functions.h"
#include "function/map/vector_map_functions.h"
#include "function/path/vector_path_functions.h"
#include "function/rdf/vector_rdf_functions.h"
#include "function/schema/vector_node_rel_functions.h"
#include "function/string/vector_string_functions.h"
#include "function/struct/vector_struct_functions.h"
#include "function/table/call_functions.h"
#include "function/timestamp/vector_timestamp_functions.h"
#include "function/union/vector_union_functions.h"
#include "function/uuid/vector_uuid_functions.h"
#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"
#include "processor/operator/persistent/reader/npy/npy_reader.h"
#include "processor/operator/persistent/reader/parquet/parquet_reader.h"
#include "processor/operator/persistent/reader/rdf/rdf_scan.h"
#include "processor/operator/table_scan/ftable_scan_function.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

void BuiltInFunctionsUtils::createFunctions(CatalogSet* catalogSet) {
    registerScalarFunctions(catalogSet);
    registerAggregateFunctions(catalogSet);
    registerTableFunctions(catalogSet);
}

void BuiltInFunctionsUtils::registerScalarFunctions(CatalogSet* catalogSet) {
    registerComparisonFunctions(catalogSet);
    registerArithmeticFunctions(catalogSet);
    registerDateFunctions(catalogSet);
    registerTimestampFunctions(catalogSet);
    registerIntervalFunctions(catalogSet);
    registerStringFunctions(catalogSet);
    registerCastFunctions(catalogSet);
    registerListFunctions(catalogSet);
    registerStructFunctions(catalogSet);
    registerMapFunctions(catalogSet);
    registerUnionFunctions(catalogSet);
    registerNodeRelFunctions(catalogSet);
    registerPathFunctions(catalogSet);
    registerBlobFunctions(catalogSet);
    registerUUIDFunctions(catalogSet);
    registerRdfFunctions(catalogSet);
}

void BuiltInFunctionsUtils::registerAggregateFunctions(CatalogSet* catalogSet) {
    registerCountStar(catalogSet);
    registerCount(catalogSet);
    registerSum(catalogSet);
    registerAvg(catalogSet);
    registerMin(catalogSet);
    registerMax(catalogSet);
    registerCollect(catalogSet);
}

Function* BuiltInFunctionsUtils::matchFunction(const std::string& name, CatalogSet* catalogSet) {
    return matchFunction(name, std::vector<common::LogicalType>{}, catalogSet);
}

Function* BuiltInFunctionsUtils::matchFunction(const std::string& name,
    const std::vector<common::LogicalType>& inputTypes, CatalogSet* catalogSet) {
    if (!catalogSet->containsEntry(name)) {
        throw CatalogException(stringFormat("{} function does not exist.", name));
    }
    auto& functionSet =
        reinterpret_cast<FunctionCatalogEntry*>(catalogSet->getEntry(name))->getFunctionSet();
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
    validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes, functionSet);
    if (candidateFunctions.size() > 1) {
        return getBestMatch(candidateFunctions);
    }
    validateSpecialCases(candidateFunctions, name, inputTypes, functionSet);
    return candidateFunctions[0];
}

AggregateFunction* BuiltInFunctionsUtils::matchAggregateFunction(const std::string& name,
    const std::vector<common::LogicalType>& inputTypes, bool isDistinct, CatalogSet* catalogSet) {
    auto& functionSet =
        reinterpret_cast<FunctionCatalogEntry*>(catalogSet->getEntry(name))->getFunctionSet();
    std::vector<AggregateFunction*> candidateFunctions;
    for (auto& function : functionSet) {
        auto aggregateFunction = ku_dynamic_cast<Function*, AggregateFunction*>(function.get());
        auto cost = getAggregateFunctionCost(inputTypes, isDistinct, aggregateFunction);
        if (cost == UINT32_MAX) {
            continue;
        }
        candidateFunctions.push_back(aggregateFunction);
    }
    validateNonEmptyCandidateFunctions(
        candidateFunctions, name, inputTypes, isDistinct, functionSet);
    KU_ASSERT(candidateFunctions.size() == 1);
    return candidateFunctions[0];
}

uint32_t BuiltInFunctionsUtils::getCastCost(LogicalTypeID inputTypeID, LogicalTypeID targetTypeID) {
    if (inputTypeID == targetTypeID) {
        return 0;
    }
    // TODO(Jiamin): should check any type
    if (inputTypeID == LogicalTypeID::ANY || targetTypeID == LogicalTypeID::ANY ||
        inputTypeID == LogicalTypeID::RDF_VARIANT) {
        // anything can be cast to ANY type for (almost no) cost
        return 1;
    }
    if (targetTypeID == LogicalTypeID::RDF_VARIANT) {
        return castFromRDFVariant(inputTypeID);
    }
    if (targetTypeID == LogicalTypeID::STRING) {
        return castFromString(inputTypeID);
    }
    switch (inputTypeID) {
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
    case LogicalTypeID::UUID:
        return castUUID(targetTypeID);
    case LogicalTypeID::SERIAL:
        return castSerial(targetTypeID);
    case LogicalTypeID::TIMESTAMP_SEC:
    case LogicalTypeID::TIMESTAMP_MS:
    case LogicalTypeID::TIMESTAMP_NS:
    case LogicalTypeID::TIMESTAMP_TZ:
        // currently don't allow timestamp to other timestamp types
        return castTimestamp(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::getTargetTypeCost(LogicalTypeID typeID) {
    switch (typeID) {
    case LogicalTypeID::INT64:
        return 101;
    case LogicalTypeID::INT32:
        return 102;
    case LogicalTypeID::INT128:
        return 103;
    case LogicalTypeID::DOUBLE:
        return 104;
    case LogicalTypeID::TIMESTAMP:
        return 120;
    case LogicalTypeID::STRING:
        return 149;
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::MAP:
    case LogicalTypeID::FIXED_LIST:
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::UNION:
        return 160;
    case LogicalTypeID::RDF_VARIANT:
        return 170;
    default:
        return 110;
    }
}

uint32_t BuiltInFunctionsUtils::castInt64(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castInt32(LogicalTypeID targetTypeID) {
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

uint32_t BuiltInFunctionsUtils::castInt16(LogicalTypeID targetTypeID) {
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

uint32_t BuiltInFunctionsUtils::castInt8(LogicalTypeID targetTypeID) {
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

uint32_t BuiltInFunctionsUtils::castUInt64(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castUInt32(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castUInt16(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castUInt8(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
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

uint32_t BuiltInFunctionsUtils::castInt128(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castUUID(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::STRING:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castDouble(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castFloat(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::DOUBLE:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castDate(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::TIMESTAMP:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castSerial(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT64:
        return 0;
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castTimestamp(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::TIMESTAMP:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castFromString(LogicalTypeID inputTypeID) {
    switch (inputTypeID) {
    case LogicalTypeID::BLOB:
    case LogicalTypeID::INTERNAL_ID:
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
        return UNDEFINED_CAST_COST;
    default: // Any other inputTypeID can be cast to String, but this cast has a high cost
        return getTargetTypeCost(LogicalTypeID::STRING);
    }
}

uint32_t BuiltInFunctionsUtils::castFromRDFVariant(LogicalTypeID inputTypeID) {
    switch (inputTypeID) {
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::FIXED_LIST:
    case LogicalTypeID::UNION:
    case LogicalTypeID::MAP:
    case LogicalTypeID::NODE:
    case LogicalTypeID::REL:
    case LogicalTypeID::RECURSIVE_REL:
    case LogicalTypeID::RDF_VARIANT:
        return UNDEFINED_CAST_COST;
    default: // Any other inputTypeID can be cast to RDF_VARIANT, but this cast has a high cost
        return getTargetTypeCost(LogicalTypeID::RDF_VARIANT);
    }
}

// When there is multiple candidates functions, e.g. double + int and double + double for input
// "1.5 + parameter", we prefer the one without any implicit casting i.e. double + double.
// Additionally, we prefer function with string parameter because string is most permissive and can
// be cast to any type.
Function* BuiltInFunctionsUtils::getBestMatch(std::vector<Function*>& functionsToMatch) {
    KU_ASSERT(functionsToMatch.size() > 1);
    Function* result = nullptr;
    auto cost = UNDEFINED_CAST_COST;
    for (auto& function : functionsToMatch) {
        auto currentCost = 0u;
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

uint32_t BuiltInFunctionsUtils::getFunctionCost(
    const std::vector<LogicalType>& inputTypes, Function* function, bool isOverload) {
    switch (function->type) {
    case FunctionType::SCALAR: {
        auto scalarFunction = ku_dynamic_cast<Function*, ScalarFunction*>(function);
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

uint32_t BuiltInFunctionsUtils::getAggregateFunctionCost(
    const std::vector<LogicalType>& inputTypes, bool isDistinct, AggregateFunction* function) {
    if (inputTypes.size() != function->parameterTypeIDs.size() ||
        isDistinct != function->isDistinct) {
        return UINT32_MAX;
    }
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        if (function->parameterTypeIDs[i] == LogicalTypeID::ANY) {
            continue;
        } else if (inputTypes[i].getLogicalTypeID() != function->parameterTypeIDs[i]) {
            return UINT32_MAX;
        }
    }
    return 0;
}

uint32_t BuiltInFunctionsUtils::matchParameters(const std::vector<LogicalType>& inputTypes,
    const std::vector<LogicalTypeID>& targetTypeIDs, bool /*isOverload*/) {
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

uint32_t BuiltInFunctionsUtils::matchVarLengthParameters(
    const std::vector<LogicalType>& inputTypes, LogicalTypeID targetTypeID, bool /*isOverload*/) {
    auto cost = 0u;
    for (auto inputType : inputTypes) {
        auto castCost = getCastCost(inputType.getLogicalTypeID(), targetTypeID);
        if (castCost == UNDEFINED_CAST_COST) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

void BuiltInFunctionsUtils::validateSpecialCases(std::vector<Function*>& candidateFunctions,
    const std::string& name, const std::vector<LogicalType>& inputTypes,
    function::function_set& set) {
    // special case for add func
    if (name == ADD_FUNC_NAME) {
        auto targetType0 = candidateFunctions[0]->parameterTypeIDs[0];
        auto targetType1 = candidateFunctions[0]->parameterTypeIDs[1];
        auto inputType0 = inputTypes[0].getLogicalTypeID();
        auto inputType1 = inputTypes[1].getLogicalTypeID();
        if ((inputType0 != LogicalTypeID::STRING || inputType1 != LogicalTypeID::STRING) &&
            targetType0 == LogicalTypeID::STRING && targetType1 == LogicalTypeID::STRING) {
            if (inputType0 != inputType1 && (inputType0 == LogicalTypeID::RDF_VARIANT ||
                                                inputType1 == LogicalTypeID::RDF_VARIANT)) {
                return;
            }
            std::string supportedInputsString;
            for (auto& function : set) {
                supportedInputsString += function->signatureToString() + "\n";
            }
            throw BinderException("Cannot match a built-in function for given function " + name +
                                  LogicalTypeUtils::toString(inputTypes) +
                                  ". Supported inputs are\n" + supportedInputsString);
        }
    }
}

void BuiltInFunctionsUtils::registerComparisonFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        EQUALS_FUNC_NAME, EqualsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        NOT_EQUALS_FUNC_NAME, NotEqualsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        GREATER_THAN_FUNC_NAME, GreaterThanFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        GREATER_THAN_EQUALS_FUNC_NAME, GreaterThanEqualsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LESS_THAN_FUNC_NAME, LessThanFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LESS_THAN_EQUALS_FUNC_NAME, LessThanEqualsFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerArithmeticFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(
        std::make_unique<ScalarFunctionCatalogEntry>(ADD_FUNC_NAME, AddFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        SUBTRACT_FUNC_NAME, SubtractFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        MULTIPLY_FUNC_NAME, MultiplyFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        DIVIDE_FUNC_NAME, DivideFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        MODULO_FUNC_NAME, ModuloFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        POWER_FUNC_NAME, PowerFunction::getFunctionSet()));
    catalogSet->createEntry(
        std::make_unique<ScalarFunctionCatalogEntry>(ABS_FUNC_NAME, AbsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ACOS_FUNC_NAME, AcosFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ASIN_FUNC_NAME, AsinFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ATAN_FUNC_NAME, AtanFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ATAN2_FUNC_NAME, Atan2Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        BITWISE_XOR_FUNC_NAME, BitwiseXorFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        BITWISE_AND_FUNC_NAME, BitwiseAndFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        BITWISE_OR_FUNC_NAME, BitwiseOrFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        BITSHIFT_LEFT_FUNC_NAME, BitShiftLeftFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        BITSHIFT_RIGHT_FUNC_NAME, BitShiftRightFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CBRT_FUNC_NAME, CbrtFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CEIL_FUNC_NAME, CeilFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CEILING_FUNC_NAME, CeilFunction::getFunctionSet()));
    catalogSet->createEntry(
        std::make_unique<ScalarFunctionCatalogEntry>(COS_FUNC_NAME, CosFunction::getFunctionSet()));
    catalogSet->createEntry(
        std::make_unique<ScalarFunctionCatalogEntry>(COT_FUNC_NAME, CotFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        DEGREES_FUNC_NAME, DegreesFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        EVEN_FUNC_NAME, EvenFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        FACTORIAL_FUNC_NAME, FactorialFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        FLOOR_FUNC_NAME, FloorFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        GAMMA_FUNC_NAME, GammaFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LGAMMA_FUNC_NAME, LgammaFunction::getFunctionSet()));
    catalogSet->createEntry(
        std::make_unique<ScalarFunctionCatalogEntry>(LN_FUNC_NAME, LnFunction::getFunctionSet()));
    catalogSet->createEntry(
        std::make_unique<ScalarFunctionCatalogEntry>(LOG_FUNC_NAME, LogFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LOG2_FUNC_NAME, Log2Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LOG10_FUNC_NAME, LogFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        NEGATE_FUNC_NAME, NegateFunction::getFunctionSet()));
    catalogSet->createEntry(
        std::make_unique<ScalarFunctionCatalogEntry>(PI_FUNC_NAME, PiFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        POW_FUNC_NAME, PowerFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        RADIANS_FUNC_NAME, RadiansFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ROUND_FUNC_NAME, RoundFunction::getFunctionSet()));
    catalogSet->createEntry(
        std::make_unique<ScalarFunctionCatalogEntry>(SIN_FUNC_NAME, SinFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        SIGN_FUNC_NAME, SignFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        SQRT_FUNC_NAME, SqrtFunction::getFunctionSet()));
    catalogSet->createEntry(
        std::make_unique<ScalarFunctionCatalogEntry>(TAN_FUNC_NAME, TanFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerDateFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        DATE_PART_FUNC_NAME, DatePartFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        DATEPART_FUNC_NAME, DatePartFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        DATE_TRUNC_FUNC_NAME, DateTruncFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        DATETRUNC_FUNC_NAME, DateTruncFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        DAYNAME_FUNC_NAME, DayNameFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        GREATEST_FUNC_NAME, GreatestFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LAST_DAY_FUNC_NAME, LastDayFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LEAST_FUNC_NAME, LeastFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        MAKE_DATE_FUNC_NAME, MakeDateFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        MONTHNAME_FUNC_NAME, MonthNameFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerTimestampFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CENTURY_FUNC_NAME, CenturyFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        EPOCH_MS_FUNC_NAME, EpochMsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TO_TIMESTAMP_FUNC_NAME, ToTimestampFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerIntervalFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TO_YEARS_FUNC_NAME, ToYearsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TO_MONTHS_FUNC_NAME, ToMonthsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TO_DAYS_FUNC_NAME, ToDaysFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TO_HOURS_FUNC_NAME, ToHoursFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TO_MINUTES_FUNC_NAME, ToMinutesFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TO_SECONDS_FUNC_NAME, ToSecondsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TO_MILLISECONDS_FUNC_NAME, ToMillisecondsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TO_MICROSECONDS_FUNC_NAME, ToMicrosecondsFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerBlobFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        OCTET_LENGTH_FUNC_NAME, OctetLengthFunctions::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ENCODE_FUNC_NAME, EncodeFunctions::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        DECODE_FUNC_NAME, DecodeFunctions::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerUUIDFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        GEN_RANDOM_UUID_FUNC_NAME, GenRandomUUIDFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerStringFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_EXTRACT_FUNC_NAME, ArrayExtractFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CONCAT_FUNC_NAME, ConcatFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CONTAINS_FUNC_NAME, ContainsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ENDS_WITH_FUNC_NAME, EndsWithFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LCASE_FUNC_NAME, LowerFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LEFT_FUNC_NAME, LeftFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LOWER_FUNC_NAME, LowerFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LPAD_FUNC_NAME, LpadFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LTRIM_FUNC_NAME, LtrimFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        PREFIX_FUNC_NAME, StartsWithFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        REPEAT_FUNC_NAME, RepeatFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        REVERSE_FUNC_NAME, ReverseFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        RIGHT_FUNC_NAME, RightFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        RPAD_FUNC_NAME, RpadFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        RTRIM_FUNC_NAME, RtrimFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        STARTS_WITH_FUNC_NAME, StartsWithFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        SUBSTR_FUNC_NAME, SubStrFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        SUBSTRING_FUNC_NAME, SubStrFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        SUFFIX_FUNC_NAME, EndsWithFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TRIM_FUNC_NAME, TrimFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        UCASE_FUNC_NAME, UpperFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        UPPER_FUNC_NAME, UpperFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        REGEXP_FULL_MATCH_FUNC_NAME, RegexpFullMatchFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        REGEXP_MATCHES_FUNC_NAME, RegexpMatchesFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        REGEXP_REPLACE_FUNC_NAME, RegexpReplaceFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        REGEXP_EXTRACT_FUNC_NAME, RegexpExtractFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        REGEXP_EXTRACT_ALL_FUNC_NAME, RegexpExtractAllFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LEVENSHTEIN_FUNC_NAME, LevenshteinFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerCastFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_DATE_FUNC_NAME, CastToDateFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_DATE_FUNC_NAME, CastToDateFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_TIMESTAMP_FUNC_NAME, CastToTimestampFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_INTERVAL_FUNC_NAME, CastToIntervalFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_INTERVAL_FUNC_NAME, CastToIntervalFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_STRING_FUNC_NAME, CastToStringFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_STRING_FUNC_NAME, CastToStringFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_BLOB_FUNC_NAME, CastToBlobFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_BLOB_FUNC_NAME, CastToBlobFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_UUID_FUNC_NAME, CastToUUIDFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_UUID_FUNC_NAME, CastToUUIDFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_DOUBLE_FUNC_NAME, CastToDoubleFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_FLOAT_FUNC_NAME, CastToFloatFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_SERIAL_FUNC_NAME, CastToSerialFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_INT64_FUNC_NAME, CastToInt64Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_INT32_FUNC_NAME, CastToInt32Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_INT16_FUNC_NAME, CastToInt16Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_INT8_FUNC_NAME, CastToInt8Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_UINT64_FUNC_NAME, CastToUInt64Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_UINT32_FUNC_NAME, CastToUInt32Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_UINT16_FUNC_NAME, CastToUInt16Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_UINT8_FUNC_NAME, CastToUInt8Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_INT128_FUNC_NAME, CastToInt128Function::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_TO_BOOL_FUNC_NAME, CastToBoolFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CAST_FUNC_NAME, CastAnyFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerListFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_CREATION_FUNC_NAME, ListCreationFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_RANGE_FUNC_NAME, ListRangeFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        SIZE_FUNC_NAME, SizeFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_EXTRACT_FUNC_NAME, ListExtractFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_ELEMENT_FUNC_NAME, ListExtractFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_CONCAT_FUNC_NAME, ListConcatFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_CAT_FUNC_NAME, ListConcatFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_CONCAT_FUNC_NAME, ListConcatFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_CAT_FUNC_NAME, ListConcatFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_APPEND_FUNC_NAME, ListAppendFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_APPEND_FUNC_NAME, ListAppendFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_PUSH_BACK_FUNC_NAME, ListAppendFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_PREPEND_FUNC_NAME, ListPrependFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_PREPEND_FUNC_NAME, ListPrependFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_PUSH_FRONT_FUNC_NAME, ListPrependFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_POSITION_FUNC_NAME, ListPositionFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_POSITION_FUNC_NAME, ListPositionFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_INDEXOF_FUNC_NAME, ListPositionFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_INDEXOF_FUNC_NAME, ListPositionFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_CONTAINS_FUNC_NAME, ListContainsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_HAS_FUNC_NAME, ListContainsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_CONTAINS_FUNC_NAME, ListContainsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_HAS_FUNC_NAME, ListContainsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_SLICE_FUNC_NAME, ListSliceFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ARRAY_SLICE_FUNC_NAME, ListSliceFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_SORT_FUNC_NAME, ListSortFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_REVERSE_SORT_FUNC_NAME, ListReverseSortFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_SUM_FUNC_NAME, ListSumFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_PRODUCT_FUNC_NAME, ListProductFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_DISTINCT_FUNC_NAME, ListDistinctFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_UNIQUE_FUNC_NAME, ListUniqueFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        LIST_ANY_VALUE_FUNC_NAME, ListAnyValueFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<catalog::ScalarFunctionCatalogEntry>(
        LIST_REVERSE_FUNC_NAME, ListReverseFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerStructFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        STRUCT_PACK_FUNC_NAME, StructPackFunctions::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        STRUCT_EXTRACT_FUNC_NAME, StructExtractFunctions::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerMapFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        MAP_CREATION_FUNC_NAME, MapCreationFunctions::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        MAP_EXTRACT_FUNC_NAME, MapExtractFunctions::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        ELEMENT_AT_FUNC_NAME, MapExtractFunctions::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        CARDINALITY_FUNC_NAME, SizeFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        MAP_KEYS_FUNC_NAME, MapKeysFunctions::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        MAP_VALUES_FUNC_NAME, MapValuesFunctions::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerUnionFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        UNION_VALUE_FUNC_NAME, UnionValueFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        UNION_TAG_FUNC_NAME, UnionTagFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        UNION_EXTRACT_FUNC_NAME, UnionExtractFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerNodeRelFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        OFFSET_FUNC_NAME, OffsetFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerPathFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        NODES_FUNC_NAME, NodesFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        RELS_FUNC_NAME, RelsFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        PROPERTIES_FUNC_NAME, PropertiesFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        IS_TRAIL_FUNC_NAME, IsTrailFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        IS_ACYCLIC_FUNC_NAME, IsACyclicFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerRdfFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        TYPE_FUNC_NAME, RDFTypeFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        VALIDATE_PREDICATE_FUNC_NAME, ValidatePredicateFunction::getFunctionSet()));
}

void BuiltInFunctionsUtils::registerCountStar(CatalogSet* catalogSet) {
    function_set functionSet;
    functionSet.push_back(std::make_unique<AggregateFunction>(COUNT_STAR_FUNC_NAME,
        std::vector<common::LogicalTypeID>{}, LogicalTypeID::INT64, CountStarFunction::initialize,
        CountStarFunction::updateAll, CountStarFunction::updatePos, CountStarFunction::combine,
        CountStarFunction::finalize, false));
    catalogSet->createEntry(std::make_unique<AggregateFunctionCatalogEntry>(
        COUNT_STAR_FUNC_NAME, std::move(functionSet)));
}

void BuiltInFunctionsUtils::registerCount(CatalogSet* catalogSet) {
    function_set functionSet;
    for (auto& type : LogicalTypeUtils::getAllValidLogicTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getAggFunc<CountFunction>(COUNT_FUNC_NAME,
                type, LogicalTypeID::INT64, isDistinct, CountFunction::paramRewriteFunc));
        }
    }
    catalogSet->createEntry(
        std::make_unique<AggregateFunctionCatalogEntry>(COUNT_FUNC_NAME, std::move(functionSet)));
}

void BuiltInFunctionsUtils::registerSum(CatalogSet* catalogSet) {
    function_set functionSet;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(
                AggregateFunctionUtil::getSumFunc(SUM_FUNC_NAME, typeID, typeID, isDistinct));
        }
    }
    catalogSet->createEntry(
        std::make_unique<AggregateFunctionCatalogEntry>(SUM_FUNC_NAME, std::move(functionSet)));
}

void BuiltInFunctionsUtils::registerAvg(CatalogSet* catalogSet) {
    function_set functionSet;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getAvgFunc(
                AVG_FUNC_NAME, typeID, LogicalTypeID::DOUBLE, isDistinct));
        }
    }
    catalogSet->createEntry(
        std::make_unique<AggregateFunctionCatalogEntry>(AVG_FUNC_NAME, std::move(functionSet)));
}

void BuiltInFunctionsUtils::registerMin(CatalogSet* catalogSet) {
    function_set functionSet;
    for (auto& type : LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getMinFunc(type, isDistinct));
        }
    }
    catalogSet->createEntry(
        std::make_unique<AggregateFunctionCatalogEntry>(MIN_FUNC_NAME, std::move(functionSet)));
}

void BuiltInFunctionsUtils::registerMax(CatalogSet* catalogSet) {
    function_set functionSet;
    for (auto& type : LogicalTypeUtils::getAllValidComparableLogicalTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            functionSet.push_back(AggregateFunctionUtil::getMaxFunc(type, isDistinct));
        }
    }
    catalogSet->createEntry(
        std::make_unique<AggregateFunctionCatalogEntry>(MAX_FUNC_NAME, std::move(functionSet)));
}

void BuiltInFunctionsUtils::registerCollect(CatalogSet* catalogSet) {
    function_set functionSet;
    for (auto isDistinct : std::vector<bool>{true, false}) {
        functionSet.push_back(std::make_unique<AggregateFunction>(COLLECT_FUNC_NAME,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::ANY}, LogicalTypeID::VAR_LIST,
            CollectFunction::initialize, CollectFunction::updateAll, CollectFunction::updatePos,
            CollectFunction::combine, CollectFunction::finalize, isDistinct,
            CollectFunction::bindFunc));
    }
    catalogSet->createEntry(
        std::make_unique<AggregateFunctionCatalogEntry>(COLLECT_FUNC_NAME, std::move(functionSet)));
}

void BuiltInFunctionsUtils::registerTableFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        CURRENT_SETTING_FUNC_NAME, CurrentSettingFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        DB_VERSION_FUNC_NAME, DBVersionFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        SHOW_TABLES_FUNC_NAME, ShowTablesFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        TABLE_INFO_FUNC_NAME, TableInfoFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        SHOW_CONNECTION_FUNC_NAME, ShowConnectionFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        STORAGE_INFO_FUNC_NAME, StorageInfoFunction::getFunctionSet()));
    // Read functions
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        READ_PARQUET_FUNC_NAME, ParquetScanFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        READ_NPY_FUNC_NAME, NpyScanFunction::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        READ_CSV_SERIAL_FUNC_NAME, SerialCSVScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        READ_CSV_PARALLEL_FUNC_NAME, ParallelCSVScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        READ_RDF_RESOURCE_FUNC_NAME, RdfResourceScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        READ_RDF_LITERAL_FUNC_NAME, RdfLiteralScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        READ_RDF_RESOURCE_TRIPLE_FUNC_NAME, RdfResourceTripleScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        READ_RDF_LITERAL_TRIPLE_FUNC_NAME, RdfLiteralTripleScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        READ_RDF_ALL_TRIPLE_FUNC_NAME, RdfAllTripleScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        IN_MEM_READ_RDF_RESOURCE_FUNC_NAME, RdfResourceInMemScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        IN_MEM_READ_RDF_LITERAL_FUNC_NAME, RdfLiteralInMemScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        IN_MEM_READ_RDF_RESOURCE_TRIPLE_FUNC_NAME, RdfResourceTripleInMemScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        IN_MEM_READ_RDF_LITERAL_TRIPLE_FUNC_NAME, RdfLiteralTripleInMemScan::getFunctionSet()));
    catalogSet->createEntry(std::make_unique<TableFunctionCatalogEntry>(
        READ_FTABLE_FUNC_NAME, FTableScan::getFunctionSet()));
}

static std::string getFunctionMatchFailureMsg(const std::string name,
    const std::vector<LogicalType>& inputTypes, const std::string& supportedInputs,
    bool isDistinct = false) {
    auto result = stringFormat("Cannot match a built-in function for given function {}{}{}.", name,
        isDistinct ? "DISTINCT " : "", LogicalTypeUtils::toString(inputTypes));
    if (supportedInputs.empty()) {
        result += " Expect empty inputs.";
    } else {
        result += " Supported inputs are\n" + supportedInputs;
    }
    return result;
}

void BuiltInFunctionsUtils::validateNonEmptyCandidateFunctions(
    std::vector<AggregateFunction*>& candidateFunctions, const std::string& name,
    const std::vector<LogicalType>& inputTypes, bool isDistinct, function::function_set& set) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& function : set) {
            auto aggregateFunction = ku_dynamic_cast<Function*, AggregateFunction*>(function.get());
            if (aggregateFunction->isDistinct) {
                supportedInputsString += "DISTINCT ";
            }
            supportedInputsString += aggregateFunction->signatureToString() + "\n";
        }
        throw BinderException(
            getFunctionMatchFailureMsg(name, inputTypes, supportedInputsString, isDistinct));
    }
}

void BuiltInFunctionsUtils::validateNonEmptyCandidateFunctions(
    std::vector<Function*>& candidateFunctions, const std::string& name,
    const std::vector<LogicalType>& inputTypes, function::function_set& set) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& function : set) {
            if (function->parameterTypeIDs.empty()) {
                continue;
            }
            supportedInputsString += function->signatureToString() + "\n";
        }
        throw BinderException(getFunctionMatchFailureMsg(name, inputTypes, supportedInputsString));
    }
}

} // namespace function
} // namespace kuzu
