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
    const unordered_set<DataTypeID>& expectedTypeIDs) {
    auto dataType = parameter.dataType;
    if (!expectedTypeIDs.contains(dataType.typeID)) {
        string expectedTypesStr;
        vector<DataType> expectedTypesVec{expectedTypeIDs.begin(), expectedTypeIDs.end()};
        auto numExpectedTypes = expectedTypeIDs.size();
        for (auto i = 0u; i < numExpectedTypes - 1; ++i) {
            expectedTypesStr += Types::dataTypeToString(expectedTypesVec[i].typeID) + ", ";
        }
        expectedTypesStr += Types::dataTypeToString(expectedTypesVec[numExpectedTypes - 1].typeID);
        throw invalid_argument(parameter.getRawName() + " has data type " +
                               Types::dataTypeToString(dataType) + ". " + expectedTypesStr +
                               " was expected.");
    }
}

} // namespace function
} // namespace graphflow
