#include "function/aggregate/built_in_aggregate_functions.h"

namespace kuzu {
namespace function {

AggregateFunctionDefinition* BuiltInAggregateFunctions::matchFunction(
    const string& name, const vector<DataType>& inputTypes, bool isDistinct) {
    auto& functionDefinitions = aggregateFunctions.at(name);
    vector<AggregateFunctionDefinition*> candidateFunctions;
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

vector<string> BuiltInAggregateFunctions::getFunctionNames() {
    vector<string> result;
    for (auto& [functionName, definitions] : aggregateFunctions) {
        result.push_back(functionName);
    }
    return result;
}

uint32_t BuiltInAggregateFunctions::getFunctionCost(
    const vector<DataType>& inputTypes, bool isDistinct, AggregateFunctionDefinition* function) {
    if (inputTypes.size() != function->parameterTypeIDs.size() ||
        isDistinct != function->isDistinct) {
        return UINT32_MAX;
    }
    // Currently all aggregate functions takes either 0 or 1 parameter. Therefore we do not allow
    // any implicit cast and require a perfect match.
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        if (inputTypes[i].typeID != function->parameterTypeIDs[i]) {
            return UINT32_MAX;
        }
    }
    return 0;
}

void BuiltInAggregateFunctions::validateNonEmptyCandidateFunctions(
    vector<AggregateFunctionDefinition*>& candidateFunctions, const string& name,
    const vector<DataType>& inputTypes, bool isDistinct) {
    if (candidateFunctions.empty()) {
        string supportedInputsString;
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
}

void BuiltInAggregateFunctions::registerCountStar() {
    vector<unique_ptr<AggregateFunctionDefinition>> definitions;
    definitions.push_back(make_unique<AggregateFunctionDefinition>(COUNT_STAR_FUNC_NAME,
        vector<DataTypeID>{}, INT64, AggregateFunctionUtil::getCountStarFunction(), false));
    aggregateFunctions.insert({COUNT_STAR_FUNC_NAME, move(definitions)});
}

void BuiltInAggregateFunctions::registerCount() {
    vector<unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto& typeID : DataType::getAllValidTypeIDs()) {
        auto inputType =
            typeID == LIST ? DataType(LIST, make_unique<DataType>(ANY)) : DataType(typeID);
        for (auto isDistinct : vector<bool>{true, false}) {
            definitions.push_back(make_unique<AggregateFunctionDefinition>(COUNT_FUNC_NAME,
                vector<DataTypeID>{typeID}, INT64,
                AggregateFunctionUtil::getCountFunction(inputType, isDistinct), isDistinct));
        }
    }
    aggregateFunctions.insert({COUNT_FUNC_NAME, move(definitions)});
}

void BuiltInAggregateFunctions::registerSum() {
    vector<unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto typeID : DataType::getNumericalTypeIDs()) {
        for (auto isDistinct : vector<bool>{true, false}) {
            definitions.push_back(make_unique<AggregateFunctionDefinition>(SUM_FUNC_NAME,
                vector<DataTypeID>{typeID}, typeID,
                AggregateFunctionUtil::getSumFunction(DataType(typeID), isDistinct), isDistinct));
        }
    }
    aggregateFunctions.insert({SUM_FUNC_NAME, move(definitions)});
}

void BuiltInAggregateFunctions::registerAvg() {
    vector<unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto typeID : DataType::getNumericalTypeIDs()) {
        for (auto isDistinct : vector<bool>{true, false}) {
            definitions.push_back(make_unique<AggregateFunctionDefinition>(AVG_FUNC_NAME,
                vector<DataTypeID>{typeID}, DOUBLE,
                AggregateFunctionUtil::getAvgFunction(DataType(typeID), isDistinct), isDistinct));
        }
    }
    aggregateFunctions.insert({AVG_FUNC_NAME, move(definitions)});
}

void BuiltInAggregateFunctions::registerMin() {
    vector<unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto typeID : vector<DataTypeID>{BOOL, INT64, DOUBLE, DATE, STRING}) {
        for (auto isDistinct : vector<bool>{true, false}) {
            definitions.push_back(make_unique<AggregateFunctionDefinition>(MIN_FUNC_NAME,
                vector<DataTypeID>{typeID}, typeID,
                AggregateFunctionUtil::getMinFunction(DataType(typeID), isDistinct), isDistinct));
        }
    }
    aggregateFunctions.insert({MIN_FUNC_NAME, move(definitions)});
}

void BuiltInAggregateFunctions::registerMax() {
    vector<unique_ptr<AggregateFunctionDefinition>> definitions;
    for (auto typeID : vector<DataTypeID>{BOOL, INT64, DOUBLE, DATE, STRING}) {
        for (auto isDistinct : vector<bool>{true, false}) {
            definitions.push_back(make_unique<AggregateFunctionDefinition>(MAX_FUNC_NAME,
                vector<DataTypeID>{typeID}, typeID,
                AggregateFunctionUtil::getMaxFunction(DataType(typeID), isDistinct), isDistinct));
        }
    }
    aggregateFunctions.insert({MAX_FUNC_NAME, move(definitions)});
}

} // namespace function
} // namespace kuzu
