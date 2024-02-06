#pragma once

#include "aggregate_function.h"
#include "scalar_function.h"

namespace kuzu {
namespace function {

class BuiltInFunctions {
public:
    BuiltInFunctions();

    FunctionType getFunctionType(const std::string& name);

    Function* matchFunction(const std::string& name);
    // TODO(Ziyi): We should have a unified interface for matching table, aggregate and scalar
    // functions.
    Function* matchFunction(
        const std::string& name, const std::vector<common::LogicalType>& inputTypes);

    AggregateFunction* matchAggregateFunction(const std::string& name,
        const std::vector<common::LogicalType>& inputTypes, bool isDistinct);

    static uint32_t getCastCost(
        common::LogicalTypeID inputTypeID, common::LogicalTypeID targetTypeID);

    void addFunction(std::string name, function::function_set definitions);

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

    static uint32_t castTimestamp(common::LogicalTypeID targetTypeID);

    static uint32_t castFromString(common::LogicalTypeID inputTypeID);

    static uint32_t castFromRDFVariant(common::LogicalTypeID inputTypeID);

    static uint32_t castUUID(common::LogicalTypeID targetTypeID);

    Function* getBestMatch(std::vector<Function*>& functions);

    uint32_t getFunctionCost(
        const std::vector<common::LogicalType>& inputTypes, Function* function, bool isOverload);
    uint32_t matchParameters(const std::vector<common::LogicalType>& inputTypes,
        const std::vector<common::LogicalTypeID>& targetTypeIDs, bool isOverload);
    uint32_t matchVarLengthParameters(const std::vector<common::LogicalType>& inputTypes,
        common::LogicalTypeID targetTypeID, bool isOverload);
    uint32_t getAggregateFunctionCost(const std::vector<common::LogicalType>& inputTypes,
        bool isDistinct, AggregateFunction* function);

    void validateSpecialCases(std::vector<Function*>& candidateFunctions, const std::string& name,
        const std::vector<common::LogicalType>& inputTypes);

    // Scalar functions.
    void registerScalarFunctions();
    void registerComparisonFunctions();
    void registerArithmeticFunctions();
    void registerDateFunctions();
    void registerTimestampFunctions();
    void registerIntervalFunctions();
    void registerBlobFunctions();
    void registerUUIDFunctions();
    void registerStringFunctions();
    void registerCastFunctions();
    void registerListFunctions();
    void registerStructFunctions();
    void registerMapFunctions();
    void registerUnionFunctions();
    void registerNodeRelFunctions();
    void registerPathFunctions();
    void registerRdfFunctions();

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

    // Validations
    void validateFunctionExists(const std::string& name);
    void validateNonEmptyCandidateFunctions(std::vector<AggregateFunction*>& candidateFunctions,
        const std::string& name, const std::vector<common::LogicalType>& inputTypes,
        bool isDistinct);
    void validateNonEmptyCandidateFunctions(std::vector<Function*>& candidateFunctions,
        const std::string& name, const std::vector<common::LogicalType>& inputTypes);

private:
    std::unordered_map<std::string, function_set> functions;
};

} // namespace function
} // namespace kuzu
