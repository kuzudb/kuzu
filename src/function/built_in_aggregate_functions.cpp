#include "function/aggregate/built_in_aggregate_functions.h"

#include "function/aggregate/collect.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

AggregateFunctionDefinition* BuiltInAggregateFunctions::matchFunction(
    const std::string& name, const std::vector<DataType>& inputTypes, bool isDistinct) {
    auto& functionDefinitions = aggregateFunctions.at(name);
    std::vector<AggregateFunctionDefinition*> candidateFunctions;
    for (auto& functionDefinition : functionDefinitions) {
        auto cost = getFunctionCost(inputTypes, isDistinct, functionDefinition.get());
        if (cost == UINT32_MAX) {
            continue;
        }
        candidateFunctions.push_back(functionDefinition.get());
    }
    validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes, isDistinct);
    assert(candidateFunctions.size() == 1);
    return candidateFunctions[0];
}

std::vector<std::string> BuiltInAggregateFunctions::getFunctionNames() {
    std::vector<std::string> result;
    for (auto& [functionName, definitions] : aggregateFunctions) {
        result.push_back(functionName);
    }
    return result;
}

uint32_t BuiltInAggregateFunctions::getFunctionCost(const std::vector<DataType>& inputTypes,
    bool isDistinct, AggregateFunctionDefinition* function) {
    if (inputTypes.size() != function->parameterTypeIDs.size() ||
        isDistinct != function->isDistinct) {
        return UINT32_MAX;
    }
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        if (function->parameterTypeIDs[i] == ANY) {
            continue;
        } else if (inputTypes[i].typeID != function->parameterTypeIDs[i]) {
            return UINT32_MAX;
        }
    }
    return 0;
}

void BuiltInAggregateFunctions::validateNonEmptyCandidateFunctions(
    std::vector<AggregateFunctionDefinition*>& candidateFunctions, const std::string& name,
    const std::vector<DataType>& inputTypes, bool isDistinct) {
    if (candidateFunctions.empty()) {
        std::string supportedInputsString;
        for (auto& functionDefinition : aggregateFunctions.at(name)) {
            if (functionDefinition->isDistinct) {
                supportedInputsString += "DISTINCT ";
            }
            supportedInputsString += functionDefinition->signatureToString() + "\n";
        }
        throw BinderException("Cannot match a built-in function for given function " + name +
                              (isDistinct ? "DISTINCT " : "") +
                              Types::dataTypesToString(inputTypes) + ". Supported inputs are\n" +
                              supportedInputsString);
    }
}

void BuiltInAggregateFunctions::registerAggregateFunctions() {
    registerCountStar();
    registerCount();
    registerSum();
    registerAvg();
    registerMin();
    registerMax();
    registerCollect();
}

void BuiltInAggregateFunctions::registerCountStar() {
    std::vector<std::unique_ptr<AggregateFunctionDefinition>> definitions;
    definitions.push_back(std::make_unique<AggregateFunctionDefinition>(COUNT_STAR_FUNC_NAME,
        std::vector<DataTypeID>{}, INT64, AggregateFunctionUtil::getCountStarFunction(), false));
    aggregateFunctions.insert({COUNT_STAR_FUNC_NAME, std::move(definitions)});
}

void BuiltInAggregateFunctions::registerCount() {
    std::vector<std::unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto& typeID : DataType::getAllValidTypeIDs()) {
        auto inputType = (typeID == VAR_LIST ? DataType(VAR_LIST, std::make_unique<DataType>(ANY)) :
                                               DataType(typeID));
        for (auto isDistinct : std::vector<bool>{true, false}) {
            definitions.push_back(std::make_unique<AggregateFunctionDefinition>(COUNT_FUNC_NAME,
                std::vector<DataTypeID>{typeID}, INT64,
                AggregateFunctionUtil::getCountFunction(inputType, isDistinct), isDistinct));
        }
    }
    aggregateFunctions.insert({COUNT_FUNC_NAME, std::move(definitions)});
}

void BuiltInAggregateFunctions::registerSum() {
    std::vector<std::unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto typeID : DataType::getNumericalTypeIDs()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            definitions.push_back(std::make_unique<AggregateFunctionDefinition>(SUM_FUNC_NAME,
                std::vector<DataTypeID>{typeID}, typeID,
                AggregateFunctionUtil::getSumFunction(DataType(typeID), isDistinct), isDistinct));
        }
    }
    aggregateFunctions.insert({SUM_FUNC_NAME, std::move(definitions)});
}

void BuiltInAggregateFunctions::registerAvg() {
    std::vector<std::unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto typeID : DataType::getNumericalTypeIDs()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            definitions.push_back(std::make_unique<AggregateFunctionDefinition>(AVG_FUNC_NAME,
                std::vector<DataTypeID>{typeID}, DOUBLE,
                AggregateFunctionUtil::getAvgFunction(DataType(typeID), isDistinct), isDistinct));
        }
    }
    aggregateFunctions.insert({AVG_FUNC_NAME, std::move(definitions)});
}

void BuiltInAggregateFunctions::registerMin() {
    std::vector<std::unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto typeID : DataType::getAllValidComparableTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            definitions.push_back(std::make_unique<AggregateFunctionDefinition>(MIN_FUNC_NAME,
                std::vector<DataTypeID>{typeID}, typeID,
                AggregateFunctionUtil::getMinFunction(DataType(typeID), isDistinct), isDistinct));
        }
    }
    aggregateFunctions.insert({MIN_FUNC_NAME, std::move(definitions)});
}

void BuiltInAggregateFunctions::registerMax() {
    std::vector<std::unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto typeID : DataType::getAllValidComparableTypes()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            definitions.push_back(std::make_unique<AggregateFunctionDefinition>(MAX_FUNC_NAME,
                std::vector<DataTypeID>{typeID}, typeID,
                AggregateFunctionUtil::getMaxFunction(DataType(typeID), isDistinct), isDistinct));
        }
    }
    aggregateFunctions.insert({MAX_FUNC_NAME, std::move(definitions)});
}

void BuiltInAggregateFunctions::registerCollect() {
    std::vector<std::unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto isDistinct : std::vector<bool>{true, false}) {
        definitions.push_back(std::make_unique<AggregateFunctionDefinition>(COLLECT_FUNC_NAME,
            std::vector<DataTypeID>{ANY}, VAR_LIST,
            AggregateFunctionUtil::getCollectFunction(DataType(ANY), isDistinct), isDistinct,
            CollectFunction::bindFunc));
    }
    aggregateFunctions.insert({COLLECT_FUNC_NAME, std::move(definitions)});
}

} // namespace function
} // namespace kuzu
