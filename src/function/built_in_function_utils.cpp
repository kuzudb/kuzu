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
#include "function/function_collection.h"
#include "function/path/vector_path_functions.h"
#include "function/rdf/vector_rdf_functions.h"
#include "function/scalar_function.h"
#include "function/schema/vector_node_rel_functions.h"
#include "function/table/call_functions.h"
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

static void validateNonEmptyCandidateFunctions(std::vector<AggregateFunction*>& candidateFunctions,
    const std::string& name, const std::vector<common::LogicalType>& inputTypes, bool isDistinct,
    function::function_set& set);
static void validateNonEmptyCandidateFunctions(std::vector<Function*>& candidateFunctions,
    const std::string& name, const std::vector<common::LogicalType>& inputTypes,
    function::function_set& set);

void BuiltInFunctionsUtils::createFunctions(CatalogSet* catalogSet) {
    registerScalarFunctions(catalogSet);
    registerAggregateFunctions(catalogSet);
    registerTableFunctions(catalogSet);

    registerFunctions(catalogSet);
}

void BuiltInFunctionsUtils::registerScalarFunctions(CatalogSet* catalogSet) {
    registerNodeRelFunctions(catalogSet);
    registerPathFunctions(catalogSet);
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
    case LogicalTypeID::ARRAY:
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
    case LogicalTypeID::ARRAY:
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
    default:
        return matchParameters(inputTypes, function->parameterTypeIDs, isOverload);
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
    if (name == AddFunction::name) {
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

void BuiltInFunctionsUtils::registerNodeRelFunctions(CatalogSet* catalogSet) {
    catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
        OFFSET_FUNC_NAME, OffsetFunction::getFunctionSet()));
    catalogSet->createEntry(
        std::make_unique<FunctionCatalogEntry>(catalog::CatalogEntryType::REWRITE_FUNCTION_ENTRY,
            ID_FUNC_NAME, IDFunction::getFunctionSet()));
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

void BuiltInFunctionsUtils::registerFunctions(catalog::CatalogSet* catalogSet) {
    auto functions = FunctionCollection::getFunctions();
    for (auto i = 0u; functions[i].name != nullptr; ++i) {
        auto functionSet = functions[i].getFunctionSetFunc();
        catalogSet->createEntry(std::make_unique<ScalarFunctionCatalogEntry>(
            functions[i].name, std::move(functionSet)));
    }
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

void validateNonEmptyCandidateFunctions(std::vector<AggregateFunction*>& candidateFunctions,
    const std::string& name, const std::vector<LogicalType>& inputTypes, bool isDistinct,
    function::function_set& set) {
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

void validateNonEmptyCandidateFunctions(std::vector<Function*>& candidateFunctions,
    const std::string& name, const std::vector<LogicalType>& inputTypes,
    function::function_set& set) {
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
