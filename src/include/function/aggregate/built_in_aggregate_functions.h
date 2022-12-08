#pragma once

#include "aggregate_function.h"

namespace kuzu {
namespace function {

class BuiltInAggregateFunctions {

public:
    BuiltInAggregateFunctions() { registerAggregateFunctions(); }

    inline bool containsFunction(const string& name) { return aggregateFunctions.contains(name); }

    AggregateFunctionDefinition* matchFunction(
        const string& name, const vector<DataType>& inputTypes, bool isDistinct);

    vector<string> getFunctionNames();

private:
    uint32_t getFunctionCost(
        const vector<DataType>& inputTypes, bool isDistinct, AggregateFunctionDefinition* function);

    void validateNonEmptyCandidateFunctions(
        vector<AggregateFunctionDefinition*>& candidateFunctions, const string& name,
        const vector<DataType>& inputTypes, bool isDistinct);

    void registerAggregateFunctions();
    void registerCountStar();
    void registerCount();
    void registerSum();
    void registerAvg();
    void registerMin();
    void registerMax();

private:
    unordered_map<string, vector<unique_ptr<AggregateFunctionDefinition>>> aggregateFunctions;
};

} // namespace function
} // namespace kuzu
