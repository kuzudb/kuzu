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

void VectorOperations::validateParameterType(
    const string& functionName, DataType inputType, DataType expectedType) {
    if (inputType != expectedType) {
        throw invalid_argument("Expected " + TypeUtils::dataTypeToString(expectedType) +
                               " type input for " + functionName + " function but get " +
                               TypeUtils::dataTypeToString(inputType) + ".");
    }
}

} // namespace function
} // namespace graphflow
