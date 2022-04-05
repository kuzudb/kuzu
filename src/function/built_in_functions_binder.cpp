#include "include/built_in_functions_binder.h"

#include "arithmetic/include/vector_arithmetic_operations.h"
#include "cast/include/vector_cast_operations.h"
#include "list/include/vector_list_operations.h"

namespace graphflow {
namespace function {

pair<scalar_exec_func, DataType> BuiltInFunctionsBinder::bindExecFunction(
    const string& functionName, const expression_vector& children) {
    validateFunctionExistence(functionName);
    switch (functionNameToIndicesMap.at(functionName)) {
    case CAST_STRING_TO_DATE: {
        return VectorCastOperations::bindCastStringToDateExecFunction(children);
    }
    case CAST_STRING_TO_TIMESTAMP: {
        return VectorCastOperations::bindCastStringToTimestampExecFunction(children);
    }
    case CAST_STRING_TO_INTERVAL: {
        return VectorCastOperations::bindCastStringToIntervalExecFunction(children);
    }
    case CAST_TO_STRING: {
        return VectorCastOperations::bindCastStructuredToStringExecFunction(children);
    }
    case LIST_CREATION: {
        return VectorListOperations::bindListCreationExecFunction(children);
    }
    case ABS: {
        return VectorArithmeticOperations::bindAbsExecFunction(children);
    }
    case FLOOR: {
        return VectorArithmeticOperations::bindFloorExecFunction(children);
    }
    case CEIL: {
        return VectorArithmeticOperations::bindCeilExecFunction(children);
    }
    case SIN: {
        return VectorArithmeticOperations::bindSinExecFunction(children);
    }
    case COS: {
        return VectorArithmeticOperations::bindCosExecFunction(children);
    }
    case TAN: {
        return VectorArithmeticOperations::bindTanExecFunction(children);
    }
    case COT: {
        return VectorArithmeticOperations::bindCotExecFunction(children);
    }
    case ASIN: {
        return VectorArithmeticOperations::bindAsinExecFunction(children);
    }
    case ACOS: {
        return VectorArithmeticOperations::bindAcosExecFunction(children);
    }
    case ATAN: {
        return VectorArithmeticOperations::bindAtanExecFunction(children);
    }
    case EVEN: {
        return VectorArithmeticOperations::bindEvenExecFunction(children);
    }
    case FACTORIAL: {
        return VectorArithmeticOperations::bindFactorialExecFunction(children);
    }
    case SIGN: {
        return VectorArithmeticOperations::bindSignExecFunction(children);
    }
    case SQRT: {
        return VectorArithmeticOperations::bindSqrtExecFunction(children);
    }
    case CBRT: {
        return VectorArithmeticOperations::bindCbrtExecFunction(children);
    }
    case GAMMA: {
        return VectorArithmeticOperations::bindGammaExecFunction(children);
    }
    case LGAMMA: {
        return VectorArithmeticOperations::bindLgammaExecFunction(children);
    }
    case LN: {
        return VectorArithmeticOperations::bindLnExecFunction(children);
    }
    case LOG: {
        return VectorArithmeticOperations::bindLogExecFunction(children);
    }
    case LOG2: {
        return VectorArithmeticOperations::bindLog2ExecFunction(children);
    }
    case DEGREES: {
        return VectorArithmeticOperations::bindDegreesExecFunction(children);
    }
    case RADIANS: {
        return VectorArithmeticOperations::bindRadiansExecFunction(children);
    }
    default:
        assert(false);
    }
}

bool BuiltInFunctionsBinder::canApplyStaticEvaluation(
    const string& functionName, const expression_vector& children) {
    validateFunctionExistence(functionName);
    switch (functionNameToIndicesMap.at(functionName)) {
    case CAST_STRING_TO_DATE:
    case CAST_STRING_TO_TIMESTAMP:
    case CAST_STRING_TO_INTERVAL: {
        if (!children.empty() && children[0]->expressionType == LITERAL_STRING) {
            return true; // bind as literal
        }
        return false;
    }
    case INTERNAL_ID:
        return true; // bind as property
    default:
        return false;
    }
}

void BuiltInFunctionsBinder::validateFunctionExistence(const string& functionName) {
    if (!functionNameToIndicesMap.contains(functionName)) {
        throw invalid_argument(functionName + " function does not exist.");
    }
}

} // namespace function
} // namespace graphflow
