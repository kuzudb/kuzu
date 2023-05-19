#pragma once

#include "aggregate_function.h"

namespace kuzu {
namespace function {

class BuiltInAggregateFunctions {

public:
    BuiltInAggregateFunctions() { registerAggregateFunctions(); }

    inline bool containsFunction(const std::string& name) {
        return aggregateFunctions.contains(name);
    }

    AggregateFunctionDefinition* matchFunction(const std::string& name,
        const std::vector<common::LogicalType>& inputTypes, bool isDistinct);

    std::vector<std::string> getFunctionNames();

private:
    uint32_t getFunctionCost(const std::vector<common::LogicalType>& inputTypes, bool isDistinct,
        AggregateFunctionDefinition* function);

    void validateNonEmptyCandidateFunctions(
        std::vector<AggregateFunctionDefinition*>& candidateFunctions, const std::string& name,
        const std::vector<common::LogicalType>& inputTypes, bool isDistinct);

    void registerAggregateFunctions();
    void registerCountStar();
    void registerCount();
    void registerSum();
    void registerAvg();
    void registerMin();
    void registerMax();
    void registerCollect();

private:
    std::unordered_map<std::string, std::vector<std::unique_ptr<AggregateFunctionDefinition>>>
        aggregateFunctions;
};

} // namespace function
} // namespace kuzu
