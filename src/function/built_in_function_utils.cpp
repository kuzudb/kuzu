#include "function/built_in_function_utils.h"

#include "catalog/catalog_entry/function_catalog_entry.h"
#include "catalog/catalog_set.h"
#include "common/exception/binder.h"
#include "common/exception/catalog.h"
#include "function/aggregate_function.h"
#include "function/arithmetic/vector_arithmetic_functions.h"
#include "function/function_collection.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

static void validateNonEmptyCandidateFunctions(std::vector<AggregateFunction*>& candidateFunctions,
    const std::string& name, const std::vector<LogicalType>& inputTypes, bool isDistinct,
    const function::function_set& set);
static void validateNonEmptyCandidateFunctions(std::vector<Function*>& candidateFunctions,
    const std::string& name, const std::vector<LogicalType>& inputTypes,
    const function::function_set& set);

void BuiltInFunctionsUtils::createFunctions(transaction::Transaction* transaction,
    CatalogSet* catalogSet) {
    auto functions = FunctionCollection::getFunctions();
    for (auto i = 0u; functions[i].name != nullptr; ++i) {
        auto functionSet = functions[i].getFunctionSetFunc();
        catalogSet->createEntry(transaction,
            std::make_unique<FunctionCatalogEntry>(functions[i].catalogEntryType, functions[i].name,
                std::move(functionSet)));
    }
}

catalog::CatalogEntry* BuiltInFunctionsUtils::getFunctionCatalogEntry(
    transaction::Transaction* transaction, const std::string& name, CatalogSet* catalogSet) {
    if (!catalogSet->containsEntry(transaction, name)) {
        throw CatalogException(stringFormat("{} function does not exist.", name));
    }
    return catalogSet->getEntry(transaction, name);
}

Function* BuiltInFunctionsUtils::matchFunction(transaction::Transaction* transaction,
    const std::string& name, CatalogSet* catalogSet) {
    return matchFunction(transaction, name, std::vector<LogicalType>{}, catalogSet);
}

Function* BuiltInFunctionsUtils::matchFunction(transaction::Transaction* transaction,
    const std::string& name, const std::vector<LogicalType>& inputTypes, CatalogSet* catalogSet) {
    auto entry = getFunctionCatalogEntry(transaction, name, catalogSet);
    return matchFunction(name, inputTypes, entry);
}

Function* BuiltInFunctionsUtils::matchFunction(const std::string& name,
    const std::vector<LogicalType>& inputTypes, const catalog::CatalogEntry* catalogEntry) {
    auto functionEntry = catalogEntry->constPtrCast<FunctionCatalogEntry>();
    auto& functionSet = functionEntry->getFunctionSet();
    std::vector<Function*> candidateFunctions;
    uint32_t minCost = UINT32_MAX;
    for (auto& function : functionSet) {
        auto func = function.get();
        auto cost = getFunctionCost(inputTypes, func);
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
    auto& functionSet = catalogSet->getEntry(&transaction::DUMMY_WRITE_TRANSACTION, name)
                            ->ptrCast<FunctionCatalogEntry>()
                            ->getFunctionSet();
    std::vector<AggregateFunction*> candidateFunctions;
    for (auto& function : functionSet) {
        auto aggregateFunction = function->ptrCast<AggregateFunction>();
        auto cost = getAggregateFunctionCost(inputTypes, isDistinct, aggregateFunction);
        if (cost == UINT32_MAX) {
            continue;
        }
        candidateFunctions.push_back(aggregateFunction);
    }
    validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes, isDistinct,
        functionSet);
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
    case LogicalTypeID::DECIMAL:
        return castDecimal(targetTypeID);
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
        // When we implement this in the future, revise tryGetMaxLogicalTypeID
        return castTimestamp(targetTypeID);
    case LogicalTypeID::LIST:
        return castList(targetTypeID);
    case LogicalTypeID::ARRAY:
        return castArray(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::getTargetTypeCost(LogicalTypeID typeID) {
    switch (typeID) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT16:
        return 100;
    case LogicalTypeID::INT64:
        return 101;
    case LogicalTypeID::INT32:
        return 102;
    case LogicalTypeID::INT128:
        return 103;
    case LogicalTypeID::DECIMAL:
        return 104;
    case LogicalTypeID::DOUBLE:
        return 105;
    case LogicalTypeID::TIMESTAMP:
        return 120;
    case LogicalTypeID::STRING:
        return 149;
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::MAP:
    case LogicalTypeID::ARRAY:
    case LogicalTypeID::LIST:
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
    case LogicalTypeID::DECIMAL:
        return getTargetTypeCost(targetTypeID);
    case LogicalTypeID::SERIAL:
        return 0;
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castInt32(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::DECIMAL:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castInt16(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::DECIMAL:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castInt8(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::DECIMAL:
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
    case LogicalTypeID::DECIMAL:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castUInt32(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::DECIMAL:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castUInt16(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT32:
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::DECIMAL:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castUInt8(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT32:
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT128:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::DECIMAL:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castInt128(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::DECIMAL:
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

uint32_t BuiltInFunctionsUtils::castDecimal(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::FLOAT:
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
    case LogicalTypeID::LIST:
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

uint32_t BuiltInFunctionsUtils::castList(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::ARRAY:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

uint32_t BuiltInFunctionsUtils::castArray(LogicalTypeID targetTypeID) {
    switch (targetTypeID) {
    case LogicalTypeID::LIST:
        return getTargetTypeCost(targetTypeID);
    default:
        return UNDEFINED_CAST_COST;
    }
}

// When there is multiple candidates functions, e.g. double + int and double + double for input
// "1.5 + parameter", we prefer the one without any implicit casting i.e. double + double.
// Additionally, we prefer function with string parameter because string is most permissive and
// can be cast to any type.
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

uint32_t BuiltInFunctionsUtils::getFunctionCost(const std::vector<LogicalType>& inputTypes,
    Function* function) {
    if (function->isVarLength) {
        KU_ASSERT(function->parameterTypeIDs.size() == 1);
        return matchVarLengthParameters(inputTypes, function->parameterTypeIDs[0]);
    }
    return matchParameters(inputTypes, function->parameterTypeIDs);
}

uint32_t BuiltInFunctionsUtils::getAggregateFunctionCost(const std::vector<LogicalType>& inputTypes,
    bool isDistinct, AggregateFunction* function) {
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
    const std::vector<LogicalTypeID>& targetTypeIDs) {
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

uint32_t BuiltInFunctionsUtils::matchVarLengthParameters(const std::vector<LogicalType>& inputTypes,
    LogicalTypeID targetTypeID) {
    auto cost = 0u;
    for (const auto& inputType : inputTypes) {
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
    const function::function_set& set) {
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
    const function::function_set& set) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& function : set) {
            auto aggregateFunction = function->constPtrCast<AggregateFunction>();
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
    const function::function_set& set) {
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
