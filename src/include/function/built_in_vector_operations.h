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
        const std::string& name, const std::vector<common::DataType>& inputTypes);

    std::vector<std::string> getFunctionNames();

    static uint32_t getCastCost(common::DataTypeID inputTypeID, common::DataTypeID targetTypeID);

    static uint32_t getCastCost(
        const common::DataType& inputType, const common::DataType& targetType);

private:
    static uint32_t getTargetTypeCost(common::DataTypeID typeID);

    static uint32_t castInt64(common::DataTypeID targetTypeID);

    static uint32_t castInt32(common::DataTypeID targetTypeID);

    static uint32_t castInt16(common::DataTypeID targetTypeID);

    static uint32_t castDouble(common::DataTypeID targetTypeID);

    static uint32_t castFloat(common::DataTypeID targetTypeID);

    VectorOperationDefinition* getBestMatch(std::vector<VectorOperationDefinition*>& functions);

    uint32_t getFunctionCost(const std::vector<common::DataType>& inputTypes,
        VectorOperationDefinition* function, bool isOverload);
    uint32_t matchParameters(const std::vector<common::DataType>& inputTypes,
        const std::vector<common::DataTypeID>& targetTypeIDs, bool isOverload);
    uint32_t matchVarLengthParameters(const std::vector<common::DataType>& inputTypes,
        common::DataTypeID targetTypeID, bool isOverload);

    void validateNonEmptyCandidateFunctions(
        std::vector<VectorOperationDefinition*>& candidateFunctions, const std::string& name,
        const std::vector<common::DataType>& inputTypes);

    void registerVectorOperations();
    void registerComparisonOperations();
    void registerArithmeticOperations();
    void registerDateOperations();
    void registerTimestampOperations();
    void registerIntervalOperations();
    void registerStringOperations();
    void registerCastOperations();
    void registerListOperations();
    void registerInternalIDOperation();
    void registerInternalOffsetOperation();

private:
    std::unordered_map<std::string, std::vector<std::unique_ptr<VectorOperationDefinition>>>
        vectorOperations;
};

} // namespace function
} // namespace kuzu
