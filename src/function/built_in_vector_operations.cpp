#include "include/built_in_vector_operations.h"

#include "arithmetic/include/vector_arithmetic_operations.h"
#include "cast/include/vector_cast_operations.h"
#include "list/include/vector_list_operations.h"

namespace graphflow {
namespace function {

void BuiltInVectorOperations::registerVectorOperations() {
    registerArithmeticOperations();
    registerCastOperations();
    registerListOperations();
    registerInternalIDOperation();
}

bool BuiltInVectorOperations::canApplyStaticEvaluation(
    const string& functionName, const expression_vector& children) {
    assert(vectorOperations.contains(functionName));
    if (functionName == ID_FUNC_NAME) {
        return true; // bind as property
    }
    if ((functionName == CAST_TO_DATE_FUNCTION_NAME ||
            functionName == CAST_TO_TIMESTAMP_FUNCTION_NAME ||
            functionName == CAST_TO_INTERVAL_FUNCTION_NAME) &&
        children[0]->expressionType == LITERAL_STRING) {
        return true; // bind as literal
    }
    return false;
}

VectorOperationDefinition* BuiltInVectorOperations::matchFunction(
    const string& name, const vector<DataType>& inputTypes) {
    validateFunctionExistence(name);
    auto& functionDefinitions = vectorOperations.at(name);
    bool isOverload = functionDefinitions.size() > 1;
    vector<VectorOperationDefinition*> candidateFunctions;
    uint32_t minCost = UINT32_MAX;
    for (auto& functionDefinition : functionDefinitions) {
        auto cost = getFunctionCost(inputTypes, functionDefinition.get(), isOverload);
        if (cost == UINT32_MAX) {
            continue;
        }
        if (cost < minCost) {
            candidateFunctions.clear();
            candidateFunctions.push_back(functionDefinition.get());
            minCost = cost;
        } else if (cost == minCost) {
            candidateFunctions.push_back(functionDefinition.get());
        }
    }
    validateNonEmptyCandidateFunctions(candidateFunctions, name, inputTypes);
    if (candidateFunctions.size() > 1) {
        return getBestMatch(candidateFunctions);
    }
    return candidateFunctions[0];
}

// When there is multiple candidates functions, e.g. double + int and double + double for input
// "1.5 + parameter", we prefer the one without any implicit casting i.e. double + double.
VectorOperationDefinition* BuiltInVectorOperations::getBestMatch(
    vector<VectorOperationDefinition*>& functions) {
    assert(functions.size() > 1);
    VectorOperationDefinition* result;
    auto cost = UINT32_MAX;
    for (auto& function : functions) {
        unordered_set<DataTypeID> distinctParameterTypes;
        for (auto& parameterTypeID : function->parameterTypeIDs) {
            if (!distinctParameterTypes.contains(parameterTypeID)) {
                distinctParameterTypes.insert(parameterTypeID);
            }
        }
        if (distinctParameterTypes.size() < cost) {
            cost = distinctParameterTypes.size();
            result = function;
        }
    }
    assert(result != nullptr);
    return result;
}

uint32_t BuiltInVectorOperations::getFunctionCost(
    const vector<DataType>& inputTypes, VectorOperationDefinition* function, bool isOverload) {
    if (function->isVarLength) {
        assert(function->parameterTypeIDs.size() == 1);
        return matchVarLengthParameters(inputTypes, function->parameterTypeIDs[0], isOverload);
    } else {
        return matchParameters(inputTypes, function->parameterTypeIDs, isOverload);
    }
}

uint32_t BuiltInVectorOperations::matchParameters(
    const vector<DataType>& inputTypes, const vector<DataTypeID>& targetTypeIDs, bool isOverload) {
    if (inputTypes.size() != targetTypeIDs.size()) {
        return UINT32_MAX;
    }
    auto containUnstructuredTypes = any_of(inputTypes.begin(), inputTypes.end(),
        [](const DataType& type) { return type.typeID == UNSTRUCTURED; });
    auto cost = 0u;
    for (auto i = 0u; i < inputTypes.size(); ++i) {
        auto castCost = castRules(
            inputTypes[i].typeID, targetTypeIDs[i], containUnstructuredTypes, !isOverload);
        if (castCost == UINT32_MAX) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

// We do not define cost for
uint32_t BuiltInVectorOperations::matchVarLengthParameters(
    const vector<DataType>& inputTypes, DataTypeID targetTypeID, bool isOverload) {
    auto containUnstructuredTypes = any_of(inputTypes.begin(), inputTypes.end(),
        [](const DataType& type) { return type.typeID == UNSTRUCTURED; });
    auto cost = 0u;
    for (auto& inputType : inputTypes) {
        auto castCost =
            castRules(inputType.typeID, targetTypeID, containUnstructuredTypes, !isOverload);
        if (castCost == UINT32_MAX) {
            return UINT32_MAX;
        }
        cost += castCost;
    }
    return cost;
}

uint32_t BuiltInVectorOperations::castRules(DataTypeID inputTypeID, DataTypeID targetTypeID,
    bool allowCastToUnstructured, bool allowCastToStructured) {
    if (inputTypeID == INVALID) {
        // INVALID type can be any type
        return 0;
    }
    if (inputTypeID != UNSTRUCTURED && targetTypeID == UNSTRUCTURED) {
        if (allowCastToUnstructured) {
            // A structured input type can be implicitly cast to UNSTRUCTURED
            return 1;
        } else {
            return UINT32_MAX;
        }
    }
    if (inputTypeID == UNSTRUCTURED && targetTypeID != UNSTRUCTURED) {
        if (allowCastToStructured) {
            // UNSTRUCTURED input type can be implicitly cast to a structured type
            return 1;
        } else {
            return UINT32_MAX;
        }
    }
    if (inputTypeID != targetTypeID) {
        // Unable to cast
        return UINT32_MAX;
    }
    return 0; // no cast needed
}

void BuiltInVectorOperations::validateFunctionExistence(const string& functionName) {
    if (!vectorOperations.contains(functionName)) {
        throw invalid_argument(functionName + " function does not exist.");
    }
}

void BuiltInVectorOperations::validateNonEmptyCandidateFunctions(
    vector<VectorOperationDefinition*>& candidateFunctions, const string& name,
    const vector<DataType>& inputTypes) {
    if (candidateFunctions.empty()) {
        string supportedInputsString;
        for (auto& functionDefinition : vectorOperations.at(name)) {
            supportedInputsString += functionDefinition->signatureToString() + "\n";
        }
        throw invalid_argument("Cannot match a built-in function for given function " + name +
                               Types::dataTypesToString(inputTypes) + ". Supported inputs are\n" +
                               supportedInputsString);
    }
}

void BuiltInVectorOperations::registerArithmeticOperations() {
    vectorOperations.insert({ADD_FUNC_NAME, AddVectorOperation::getDefinitions()});
    vectorOperations.insert({SUBTRACT_FUNC_NAME, SubtractVectorOperation::getDefinitions()});
    vectorOperations.insert({MULTIPLY_FUNC_NAME, MultiplyVectorOperation::getDefinitions()});
    vectorOperations.insert({DIVIDE_FUNC_NAME, DivideVectorOperation::getDefinitions()});
    vectorOperations.insert({MODULO_FUNC_NAME, ModuloVectorOperation::getDefinitions()});
    vectorOperations.insert({POWER_FUNC_NAME, PowerVectorOperation::getDefinitions()});

    vectorOperations.insert({NEGATE_FUNC_NAME, NegateVectorOperation::getDefinitions()});
    vectorOperations.insert({ABS_FUNC_NAME, AbsVectorOperation::getDefinitions()});
    vectorOperations.insert({FLOOR_FUNC_NAME, FloorVectorOperation::getDefinitions()});
    vectorOperations.insert({CEIL_FUNC_NAME, CeilVectorOperation::getDefinitions()});

    vectorOperations.insert({SIN_FUNC_NAME, SinVectorOperation::getDefinitions()});
    vectorOperations.insert({COS_FUNC_NAME, CosVectorOperation::getDefinitions()});
    vectorOperations.insert({TAN_FUNC_NAME, TanVectorOperation::getDefinitions()});
    vectorOperations.insert({COT_FUNC_NAME, CotVectorOperation::getDefinitions()});
    vectorOperations.insert({ASIN_FUNC_NAME, AsinVectorOperation::getDefinitions()});
    vectorOperations.insert({ACOS_FUNC_NAME, AcosVectorOperation::getDefinitions()});
    vectorOperations.insert({ATAN_FUNC_NAME, AtanVectorOperation::getDefinitions()});

    vectorOperations.insert({FACTORIAL_FUNC_NAME, FactorialVectorOperation::getDefinitions()});
    vectorOperations.insert({SQRT_FUNC_NAME, SqrtVectorOperation::getDefinitions()});
    vectorOperations.insert({CBRT_FUNC_NAME, CbrtVectorOperation::getDefinitions()});
    vectorOperations.insert({GAMMA_FUNC_NAME, GammaVectorOperation::getDefinitions()});
    vectorOperations.insert({LGAMMA_FUNC_NAME, LgammaVectorOperation::getDefinitions()});
    vectorOperations.insert({LN_FUNC_NAME, LnVectorOperation::getDefinitions()});
    vectorOperations.insert({LOG_FUNC_NAME, LogVectorOperation::getDefinitions()});
    vectorOperations.insert({LOG2_FUNC_NAME, Log2VectorOperation::getDefinitions()});
    vectorOperations.insert({DEGREES_FUNC_NAME, DegreesVectorOperation::getDefinitions()});
    vectorOperations.insert({RADIANS_FUNC_NAME, RadiansVectorOperation::getDefinitions()});
    vectorOperations.insert({EVEN_FUNC_NAME, EvenVectorOperation::getDefinitions()});
    vectorOperations.insert({SIGN_FUNC_NAME, SignVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerCastOperations() {
    vectorOperations.insert(
        {CAST_TO_DATE_FUNCTION_NAME, CastToDateVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_TIMESTAMP_FUNCTION_NAME, CastToTimestampVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_INTERVAL_FUNCTION_NAME, CastToIntervalVectorOperation::getDefinitions()});
    vectorOperations.insert(
        {CAST_TO_STRING_FUNCTION_NAME, CastToStringVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerListOperations() {
    vectorOperations.insert(
        {LIST_CREATION_FUNC_NAME, ListCreationVectorOperation::getDefinitions()});
}

void BuiltInVectorOperations::registerInternalIDOperation() {
    vector<unique_ptr<VectorOperationDefinition>> definitions;
    auto definition = make_unique<VectorOperationDefinition>(
        ID_FUNC_NAME, vector<DataTypeID>{NODE}, NODE_ID, nullptr);
    definitions.push_back(move(definition));
    vectorOperations.insert({ID_FUNC_NAME, move(definitions)});
}

} // namespace function
} // namespace graphflow
