#include "include/vector_operations.h"

namespace graphflow {
namespace function {

void VectorOperations::validateNumParameters(
    const string& functionName, uint64_t inputNumParams, uint64_t expectedNumParams) {
    if (inputNumParams != expectedNumParams) {
        throw invalid_argument("Expected " + to_string(expectedNumParams) + " parameters for " +
                               functionName + " function but get " + to_string(inputNumParams) +
                               ".");
    }
}

void VectorOperations::validateParameterType(const string& functionName, Expression& parameter,
    const unordered_set<DataType>& expectedTypes) {
    auto dataType = parameter.dataType;
    if (!expectedTypes.contains(dataType)) {
        string expectedTypesStr;
        vector<DataType> expectedTypesVec{expectedTypes.begin(), expectedTypes.end()};
        auto numExpectedTypes = expectedTypes.size();
        for (auto i = 0u; i < numExpectedTypes - 1; ++i) {
            expectedTypesStr += TypeUtils::dataTypeToString(expectedTypesVec[i]) + ", ";
        }
        expectedTypesStr += TypeUtils::dataTypeToString(expectedTypesVec[numExpectedTypes - 1]);
        throw invalid_argument(parameter.getRawName() + " has data type " +
                               TypeUtils::dataTypeToString(dataType) + ". " + expectedTypesStr +
                               " was expected.");
    }
}

} // namespace function
} // namespace graphflow
