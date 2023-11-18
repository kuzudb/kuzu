#pragma once

#include "aggregate_function.h"
#include "scalar_function.h"

namespace kuzu {
namespace function {

class BuiltInFunctions {

public:
    BuiltInFunctions();

    inline bool containsFunction(const std::string& functionName) {
        return functions.contains(functionName);
    }

    FunctionType getFunctionType(const std::string& functionName) {
        KU_ASSERT(containsFunction(functionName));
        return functions.at(functionName)[0]->type;
    }

    // TODO(Ziyi): We should have a unified interface for matching table, aggregate and scalar
    // functions.
    Function* matchScalarFunction(
        const std::string& name, const std::vector<common::LogicalType*>& inputTypes);

    AggregateFunction* matchAggregateFunction(const std::string& name,
        const std::vector<common::LogicalType*>& inputTypes, bool isDistinct);

    static uint32_t getCastCost(
        common::LogicalTypeID inputTypeID, common::LogicalTypeID targetTypeID);

    void addFunction(std::string name, function::function_set definitions);

    uint32_t getAggregateFunctionCost(const std::vector<common::LogicalType*>& inputTypes,
        bool isDistinct, AggregateFunction* function);

    void validateNonEmptyCandidateFunctions(std::vector<AggregateFunction*>& candidateFunctions,
        const std::string& name, const std::vector<common::LogicalType*>& inputTypes,
        bool isDistinct);

    std::unique_ptr<BuiltInFunctions> copy();

private:
    static uint32_t getTargetTypeCost(common::LogicalTypeID typeID);

    static uint32_t castInt64(common::LogicalTypeID targetTypeID);

    static uint32_t castInt32(common::LogicalTypeID targetTypeID);

    static uint32_t castInt16(common::LogicalTypeID targetTypeID);

    static uint32_t castInt8(common::LogicalTypeID targetTypeID);

    static uint32_t castUInt64(common::LogicalTypeID targetTypeID);

    static uint32_t castUInt32(common::LogicalTypeID targetTypeID);

    static uint32_t castUInt16(common::LogicalTypeID targetTypeID);

    static uint32_t castUInt8(common::LogicalTypeID targetTypeID);

    static uint32_t castInt128(common::LogicalTypeID targetTypeID);

    static uint32_t castDouble(common::LogicalTypeID targetTypeID);

    static uint32_t castFloat(common::LogicalTypeID targetTypeID);

    static uint32_t castDate(common::LogicalTypeID targetTypeID);

    static uint32_t castSerial(common::LogicalTypeID targetTypeID);

    Function* getBestMatch(std::vector<Function*>& functions);

    uint32_t getFunctionCost(
        const std::vector<common::LogicalType*>& inputTypes, Function* function, bool isOverload);
    uint32_t matchParameters(const std::vector<common::LogicalType*>& inputTypes,
        const std::vector<common::LogicalTypeID>& targetTypeIDs, bool isOverload);
    uint32_t matchVarLengthParameters(const std::vector<common::LogicalType*>& inputTypes,
        common::LogicalTypeID targetTypeID, bool isOverload);

    void validateNonEmptyCandidateFunctions(std::vector<Function*>& candidateFunctions,
        const std::string& name, const std::vector<common::LogicalType*>& inputTypes);

    // Scalar functions.
    void registerScalarFunctions();
    void registerComparisonFunctions();
    void registerArithmeticFunctions();
    void registerDateFunctions();
    void registerTimestampFunctions();
    void registerIntervalFunctions();
    void registerBlobFunctions();
    void registerStringFunctions();
    void registerCastFunctions();
    void registerListFunctions();
    void registerStructFunctions();
    void registerMapFunctions();
    void registerUnionFunctions();
    void registerNodeRelFunctions();
    void registerPathFunctions();

    // Aggregate functions.
    void registerAggregateFunctions();
    void registerCountStar();
    void registerCount();
    void registerSum();
    void registerAvg();
    void registerMin();
    void registerMax();
    void registerCollect();

    // Table functions.
    void registerTableFunctions();

private:
    std::unordered_map<std::string, function_set> functions;
};

} // namespace function
} // namespace kuzu
