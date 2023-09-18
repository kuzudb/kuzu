#pragma once

#include "vector_functions.h"

namespace kuzu {
namespace function {

class BuiltInVectorFunctions {

public:
    BuiltInVectorFunctions() { registerVectorFunctions(); }

    inline bool containsFunction(const std::string& functionName) {
        return vectorFunctions.contains(functionName);
    }

    VectorFunctionDefinition* matchVectorFunction(
        const std::string& name, const std::vector<common::LogicalType>& inputTypes);

    static uint32_t getCastCost(
        common::LogicalTypeID inputTypeID, common::LogicalTypeID targetTypeID);

    void addFunction(std::string name, function::vector_function_definitions definitions);

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

    static uint32_t castDouble(common::LogicalTypeID targetTypeID);

    static uint32_t castFloat(common::LogicalTypeID targetTypeID);

    static uint32_t castDate(common::LogicalTypeID targetTypeID);

    static uint32_t castSerial(common::LogicalTypeID targetTypeID);

    VectorFunctionDefinition* getBestMatch(std::vector<VectorFunctionDefinition*>& functions);

    uint32_t getFunctionCost(const std::vector<common::LogicalType>& inputTypes,
        VectorFunctionDefinition* function, bool isOverload);
    uint32_t matchParameters(const std::vector<common::LogicalType>& inputTypes,
        const std::vector<common::LogicalTypeID>& targetTypeIDs, bool isOverload);
    uint32_t matchVarLengthParameters(const std::vector<common::LogicalType>& inputTypes,
        common::LogicalTypeID targetTypeID, bool isOverload);

    void validateNonEmptyCandidateFunctions(
        std::vector<VectorFunctionDefinition*>& candidateFunctions, const std::string& name,
        const std::vector<common::LogicalType>& inputTypes);

    void registerVectorFunctions();
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

private:
    // TODO(Ziyi): Refactor VectorFunction/tableOperation to inherit from the same base class.
    std::unordered_map<std::string, vector_function_definitions> vectorFunctions;
};

} // namespace function
} // namespace kuzu
