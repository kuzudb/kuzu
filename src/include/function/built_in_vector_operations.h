#pragma once

#include "vector_operations.h"

namespace kuzu {
namespace function {

class BuiltInVectorOperations {

public:
    BuiltInVectorOperations() { registerVectorOperations(); }

    inline bool containsFunction(const string& functionName) {
        return vectorOperations.contains(functionName);
    }

    /**
     * Certain function can be evaluated statically and thus avoid runtime execution.
     * E.g. date("2021-01-01") can be evaluated as date literal statically.
     */
    bool canApplyStaticEvaluation(const string& functionName, const expression_vector& children);

    VectorOperationDefinition* matchFunction(
        const string& name, const vector<DataType>& inputTypes);

    vector<string> getFunctionNames();

private:
    VectorOperationDefinition* getBestMatch(vector<VectorOperationDefinition*>& functions);

    uint32_t getFunctionCost(
        const vector<DataType>& inputTypes, VectorOperationDefinition* function, bool isOverload);
    uint32_t matchParameters(const vector<DataType>& inputTypes,
        const vector<DataTypeID>& targetTypeIDs, bool isOverload);
    uint32_t matchVarLengthParameters(
        const vector<DataType>& inputTypes, DataTypeID targetTypeID, bool isOverload);
    uint32_t castRules(DataTypeID inputTypeID, DataTypeID targetTypeID);

    void validateNonEmptyCandidateFunctions(vector<VectorOperationDefinition*>& candidateFunctions,
        const string& name, const vector<DataType>& inputTypes);

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
    unordered_map<string, vector<unique_ptr<VectorOperationDefinition>>> vectorOperations;
};

} // namespace function
} // namespace kuzu
