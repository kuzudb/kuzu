#pragma once

#include "vector_operations.h"

namespace kuzu {
namespace function {

class BuiltInVectorOperations {

public:
    BuiltInVectorOperations() { registerVectorOperations(); }

    inline bool containsFunction(const std::string& functionName) {
        return vectorOperations.contains(functionName);
    }

    /**
     * Certain function can be evaluated statically and thus avoid runtime execution.
     * E.g. date("2021-01-01") can be evaluated as date literal statically.
     */
    bool canApplyStaticEvaluation(
        const std::string& functionName, const binder::expression_vector& children);

    VectorOperationDefinition* matchFunction(
        const std::string& name, const std::vector<common::LogicalType>& inputTypes);

    std::vector<std::string> getFunctionNames();

    static uint32_t getCastCost(
        common::LogicalTypeID inputTypeID, common::LogicalTypeID targetTypeID);

    static uint32_t getCastCost(
        const common::LogicalType& inputType, const common::LogicalType& targetType);

private:
    static uint32_t getTargetTypeCost(common::LogicalTypeID typeID);

    static uint32_t castInt64(common::LogicalTypeID targetTypeID);

    static uint32_t castInt32(common::LogicalTypeID targetTypeID);

    static uint32_t castInt16(common::LogicalTypeID targetTypeID);

    static uint32_t castDouble(common::LogicalTypeID targetTypeID);

    static uint32_t castFloat(common::LogicalTypeID targetTypeID);

    static uint32_t castDate(common::LogicalTypeID targetTypeID);

    static uint32_t castSerial(common::LogicalTypeID targetTypeID);

    VectorOperationDefinition* getBestMatch(std::vector<VectorOperationDefinition*>& functions);

    uint32_t getFunctionCost(const std::vector<common::LogicalType>& inputTypes,
        VectorOperationDefinition* function, bool isOverload);
    uint32_t matchParameters(const std::vector<common::LogicalType>& inputTypes,
        const std::vector<common::LogicalTypeID>& targetTypeIDs, bool isOverload);
    uint32_t matchVarLengthParameters(const std::vector<common::LogicalType>& inputTypes,
        common::LogicalTypeID targetTypeID, bool isOverload);

    void validateNonEmptyCandidateFunctions(
        std::vector<VectorOperationDefinition*>& candidateFunctions, const std::string& name,
        const std::vector<common::LogicalType>& inputTypes);

    void registerVectorOperations();
    void registerComparisonOperations();
    void registerArithmeticOperations();
    void registerDateOperations();
    void registerTimestampOperations();
    void registerIntervalOperations();
    void registerBlobOperations();
    void registerStringOperations();
    void registerCastOperations();
    void registerListOperations();
    void registerStructOperations();
    void registerMapOperations();
    void registerUnionOperations();
    void registerNodeRelOperations();

private:
    std::unordered_map<std::string, vector_operation_definitions> vectorOperations;
};

} // namespace function
} // namespace kuzu
