#include "include/vector_arithmetic_operations.h"

#include "operations/include/arithmetic_operations.h"

namespace graphflow {
namespace function {

static DataTypeID resolveResultType(DataTypeID leftTypeID, DataTypeID rightTypeID) {
    if (leftTypeID == DOUBLE || rightTypeID == DOUBLE) {
        return DOUBLE;
    }
    return INT64;
}

vector<unique_ptr<VectorOperationDefinition>> AddVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
        for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Add>(ADD_FUNC_NAME, leftTypeID,
                rightTypeID, resolveResultType(leftTypeID, rightTypeID)));
        }
    }
    result.push_back(getBinaryDefinition<operation::Add>(
        ADD_FUNC_NAME, UNSTRUCTURED, UNSTRUCTURED, UNSTRUCTURED));
    // string + string -> string
    result.push_back(
        make_unique<VectorOperationDefinition>(ADD_FUNC_NAME, vector<DataTypeID>{STRING, STRING},
            STRING, BinaryExecFunction<gf_string_t, gf_string_t, gf_string_t, operation::Concat>));
    // date + int → date
    result.push_back(
        make_unique<VectorOperationDefinition>(ADD_FUNC_NAME, vector<DataTypeID>{DATE, INT64}, DATE,
            BinaryExecFunction<date_t, int64_t, date_t, operation::Add>));
    // date + interval → date
    result.push_back(
        make_unique<VectorOperationDefinition>(ADD_FUNC_NAME, vector<DataTypeID>{DATE, INTERVAL},
            DATE, BinaryExecFunction<date_t, interval_t, date_t, operation::Add>));
    // timestamp + interval → timestamp
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        vector<DataTypeID>{TIMESTAMP, INTERVAL}, TIMESTAMP,
        BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Add>));
    // interval + interval → interval
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        vector<DataTypeID>{INTERVAL, INTERVAL}, INTERVAL,
        BinaryExecFunction<interval_t, interval_t, interval_t, operation::Add>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> SubtractVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
        for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Subtract>(SUBTRACT_FUNC_NAME,
                leftTypeID, rightTypeID, resolveResultType(leftTypeID, rightTypeID)));
        }
    }
    result.push_back(getBinaryDefinition<operation::Subtract>(
        SUBTRACT_FUNC_NAME, UNSTRUCTURED, UNSTRUCTURED, UNSTRUCTURED));
    // date - date → integer
    result.push_back(
        make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME, vector<DataTypeID>{DATE, DATE},
            INT64, BinaryExecFunction<date_t, date_t, int64_t, operation::Subtract>));
    // date - integer → date
    result.push_back(
        make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME, vector<DataTypeID>{DATE, INT64},
            DATE, BinaryExecFunction<date_t, int64_t, date_t, operation::Subtract>));
    // date - interval → date
    result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
        vector<DataTypeID>{DATE, INTERVAL}, DATE,
        BinaryExecFunction<date_t, interval_t, date_t, operation::Subtract>));
    // timestamp - timestamp → interval
    result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
        vector<DataTypeID>{TIMESTAMP, TIMESTAMP}, INTERVAL,
        BinaryExecFunction<timestamp_t, timestamp_t, interval_t, operation::Subtract>));
    // timestamp - interval → timestamp
    result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
        vector<DataTypeID>{TIMESTAMP, INTERVAL}, TIMESTAMP,
        BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Subtract>));
    // interval - interval → interval
    result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
        vector<DataTypeID>{INTERVAL, INTERVAL}, INTERVAL,
        BinaryExecFunction<interval_t, interval_t, interval_t, operation::Subtract>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> MultiplyVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
        for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Multiply>(MULTIPLY_FUNC_NAME,
                leftTypeID, rightTypeID, resolveResultType(leftTypeID, rightTypeID)));
        }
    }
    result.push_back(getBinaryDefinition<operation::Multiply>(
        MULTIPLY_FUNC_NAME, UNSTRUCTURED, UNSTRUCTURED, UNSTRUCTURED));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> DivideVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftType : DataType::getNumericalTypeIDs()) {
        for (auto& rightType : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Divide>(
                DIVIDE_FUNC_NAME, leftType, rightType, resolveResultType(leftType, rightType)));
        }
    }
    result.push_back(getBinaryDefinition<operation::Divide>(
        DIVIDE_FUNC_NAME, UNSTRUCTURED, UNSTRUCTURED, UNSTRUCTURED));
    // interval / int → interval
    result.push_back(make_unique<VectorOperationDefinition>(DIVIDE_FUNC_NAME,
        vector<DataTypeID>{INTERVAL, INT64}, INTERVAL,
        BinaryExecFunction<interval_t, int64_t, interval_t, operation::Divide>));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> ModuloVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
        for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Modulo>(MODULO_FUNC_NAME, leftTypeID,
                rightTypeID, resolveResultType(leftTypeID, rightTypeID)));
        }
    }
    result.push_back(getBinaryDefinition<operation::Modulo>(
        MODULO_FUNC_NAME, UNSTRUCTURED, UNSTRUCTURED, UNSTRUCTURED));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> PowerVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
        for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Power, true>(
                POWER_FUNC_NAME, leftTypeID, rightTypeID, DOUBLE));
        }
    }
    result.push_back(getBinaryDefinition<operation::Power>(
        POWER_FUNC_NAME, UNSTRUCTURED, UNSTRUCTURED, UNSTRUCTURED));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> NegateVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Negate>(NEGATE_FUNC_NAME, typeID, typeID));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> AbsVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Abs>(ABS_FUNC_NAME, typeID, typeID));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> FloorVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Floor>(FLOOR_FUNC_NAME, typeID, typeID));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> CeilVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Ceil>(CEIL_FUNC_NAME, typeID, typeID));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> SinVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Sin, false, true>(SIN_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> CosVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Cos, false, true>(COS_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> TanVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Tan, false, true>(TAN_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> CotVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Cot, false, true>(COT_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> AsinVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Asin, false, true>(ASIN_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> AcosVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Acos, false, true>(ACOS_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> AtanVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Atan, false, true>(ATAN_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> FactorialVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Factorial>(FACTORIAL_FUNC_NAME, typeID, typeID));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> SqrtVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Sqrt, false, true>(SQRT_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> CbrtVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Cbrt, false, true>(CBRT_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> GammaVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Gamma>(GAMMA_FUNC_NAME, typeID, typeID));
    }
    result.push_back(
        getUnaryDefinition<operation::Gamma>(GAMMA_FUNC_NAME, UNSTRUCTURED, UNSTRUCTURED));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> LgammaVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Lgamma, false, true>(LGAMMA_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> LnVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Ln, false, true>(LN_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> LogVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Log, false, true>(LOG_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> Log2VectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Log2, false, true>(LOG2_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> DegreesVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Degrees, false, true>(DEGREES_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> RadiansVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Radians, false, true>(RADIANS_FUNC_NAME, typeID, DOUBLE));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> EvenVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Even, true, false>(EVEN_FUNC_NAME, typeID, INT64));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> SignVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalAndUnstructuredTypeIDs()) {
        result.push_back(
            getUnaryDefinition<operation::Sign, true, false>(SIGN_FUNC_NAME, typeID, INT64));
    }
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> Atan2VectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
        for (auto& rightTypeID : DataType::getNumericalTypeIDs()) {
            result.push_back(getBinaryDefinition<operation::Atan2, true /* DOUBLE_RESULT */,
                true /* FIXED_RESULT_TYPE */>(ATAN2_FUNC_NAME, leftTypeID, rightTypeID, DOUBLE));
        }
    }
    result.push_back(getBinaryDefinition<operation::Atan2, true /* DOUBLE_RESULT */,
        true /* FIXED_RESULT_TYPE */>(ATAN2_FUNC_NAME, UNSTRUCTURED, UNSTRUCTURED, DOUBLE));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> RoundVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    for (auto& leftTypeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Round, true /* DOUBLE_RESULT */,
            true /* FIXED_RESULT_TYPE */>(ROUND_FUNC_NAME, leftTypeID, INT64, DOUBLE));
    }
    result.push_back(getBinaryDefinition<operation::Round, true /* DOUBLE_RESULT */,
        true /* FIXED_RESULT_TYPE */>(ROUND_FUNC_NAME, UNSTRUCTURED, UNSTRUCTURED, DOUBLE));
    return result;
}

vector<unique_ptr<VectorOperationDefinition>> BitwiseXorVectorOperation::getDefinitions() {
    vector<unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(BITWISE_XOR_FUNC_NAME,
        vector<DataTypeID>{INT64, INT64}, INT64,
        BinaryExecFunction<int64_t, int64_t, int64_t, operation::BitwiseXor>));
    return result;
}

} // namespace function
} // namespace graphflow
