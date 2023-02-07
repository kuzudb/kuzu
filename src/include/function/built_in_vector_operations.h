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

private:
    VectorOperationDefinition* getBestMatch(std::vector<VectorOperationDefinition*>& functions);

    uint32_t getFunctionCost(const std::vector<common::DataType>& inputTypes,
        VectorOperationDefinition* function, bool isOverload);
    uint32_t matchParameters(const std::vector<common::DataType>& inputTypes,
        const std::vector<common::DataTypeID>& targetTypeIDs, bool isOverload);
    uint32_t matchVarLengthParameters(const std::vector<common::DataType>& inputTypes,
        common::DataTypeID targetTypeID, bool isOverload);
    uint32_t castRules(common::DataTypeID inputTypeID, common::DataTypeID targetTypeID);

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

private:
    std::unordered_map<std::string, std::vector<std::unique_ptr<VectorOperationDefinition>>>
        vectorOperations;
};

} // namespace function
} // namespace kuzu
