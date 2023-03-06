#include "function/arithmetic/vector_arithmetic_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

std::vector<std::unique_ptr<VectorOperationDefinition>> AddVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Add>(ADD_FUNC_NAME, typeID));
    }
    // interval + interval → interval
    result.push_back(getBinaryDefinition<operation::Add, interval_t, interval_t>(
        ADD_FUNC_NAME, INTERVAL, INTERVAL));
    // date + int → date
    result.push_back(
        make_unique<VectorOperationDefinition>(ADD_FUNC_NAME, std::vector<DataTypeID>{DATE, INT64},
            DATE, BinaryExecFunction<date_t, int64_t, date_t, operation::Add>));
    // int + date → date
    result.push_back(
        make_unique<VectorOperationDefinition>(ADD_FUNC_NAME, std::vector<DataTypeID>{INT64, DATE},
            DATE, BinaryExecFunction<int64_t, date_t, date_t, operation::Add>));
    // date + interval → date
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        std::vector<DataTypeID>{DATE, INTERVAL}, DATE,
        BinaryExecFunction<date_t, interval_t, date_t, operation::Add>));
    // interval + date → date
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        std::vector<DataTypeID>{INTERVAL, DATE}, DATE,
        BinaryExecFunction<interval_t, date_t, date_t, operation::Add>));
    // timestamp + interval → timestamp
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        std::vector<DataTypeID>{TIMESTAMP, INTERVAL}, TIMESTAMP,
        BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Add>));
    // interval + timestamp → timestamp
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        std::vector<DataTypeID>{INTERVAL, TIMESTAMP}, TIMESTAMP,
        BinaryExecFunction<interval_t, timestamp_t, timestamp_t, operation::Add>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> SubtractVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Subtract>(SUBTRACT_FUNC_NAME, typeID));
    }
    // date - date → int64
    result.push_back(
        getBinaryDefinition<operation::Subtract, date_t, int64_t>(SUBTRACT_FUNC_NAME, DATE, INT64));
    // date - integer → date
    result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
        std::vector<DataTypeID>{DATE, INT64}, DATE,
        BinaryExecFunction<date_t, int64_t, date_t, operation::Subtract>));
    // date - interval → date
    result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
        std::vector<DataTypeID>{DATE, INTERVAL}, DATE,
        BinaryExecFunction<date_t, interval_t, date_t, operation::Subtract>));
    // timestamp - timestamp → interval
    result.push_back(getBinaryDefinition<operation::Subtract, timestamp_t, interval_t>(
        SUBTRACT_FUNC_NAME, TIMESTAMP, INTERVAL));
    // timestamp - interval → timestamp
    result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
        std::vector<DataTypeID>{TIMESTAMP, INTERVAL}, TIMESTAMP,
        BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Subtract>));
    // interval - interval → interval
    result.push_back(getBinaryDefinition<operation::Subtract, interval_t, interval_t>(
        SUBTRACT_FUNC_NAME, INTERVAL, INTERVAL));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> MultiplyVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Multiply>(MULTIPLY_FUNC_NAME, typeID));
    }
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> DivideVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Divide>(DIVIDE_FUNC_NAME, typeID));
    }
    // interval / int → interval
    result.push_back(make_unique<VectorOperationDefinition>(DIVIDE_FUNC_NAME,
        std::vector<DataTypeID>{INTERVAL, INT64}, INTERVAL,
        BinaryExecFunction<interval_t, int64_t, interval_t, operation::Divide>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ModuloVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Modulo>(MODULO_FUNC_NAME, typeID));
    }
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> PowerVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    // double_t ^ double_t -> double_t
    result.push_back(
        getBinaryDefinition<operation::Power, double_t>(POWER_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> NegateVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Negate>(NEGATE_FUNC_NAME, typeID));
    }
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> AbsVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Abs>(ABS_FUNC_NAME, typeID));
    }
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> FloorVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Floor>(FLOOR_FUNC_NAME, typeID));
    }
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> CeilVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    for (auto& typeID : DataType::getNumericalTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Ceil>(CEIL_FUNC_NAME, typeID));
    }
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> SinVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Sin, double_t>(SIN_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> CosVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Cos, double_t>(COS_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> TanVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Tan, double_t>(TAN_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> CotVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Cot, double_t>(COT_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> AsinVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Asin, double_t>(ASIN_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> AcosVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Acos, double_t>(ACOS_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> AtanVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Atan, double_t>(ATAN_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> FactorialVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        make_unique<VectorOperationDefinition>(FACTORIAL_FUNC_NAME, std::vector<DataTypeID>{INT64},
            INT64, UnaryExecFunction<int64_t, int64_t, operation::Factorial>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> SqrtVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Sqrt, double_t>(SQRT_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> CbrtVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Cbrt, double_t>(CBRT_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> GammaVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        getUnaryDefinition<operation::Gamma, double_t>(GAMMA_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LgammaVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        getUnaryDefinition<operation::Lgamma, double_t>(LGAMMA_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LnVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Ln, double_t>(LN_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LogVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Log, double_t>(LOG_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> Log2VectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Log2, double_t>(LOG2_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> DegreesVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        getUnaryDefinition<operation::Degrees, double_t>(DEGREES_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RadiansVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        getUnaryDefinition<operation::Radians, double_t>(RADIANS_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> EvenVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Even, double_t>(EVEN_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> SignVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getUnaryDefinition<operation::Sign, int64_t>(SIGN_FUNC_NAME, INT64, INT64));
    result.push_back(getUnaryDefinition<operation::Sign, int64_t>(SIGN_FUNC_NAME, DOUBLE, INT64));
    result.push_back(getUnaryDefinition<operation::Sign, int64_t>(SIGN_FUNC_NAME, FLOAT, INT64));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> Atan2VectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        getBinaryDefinition<operation::Atan2, double_t>(ATAN2_FUNC_NAME, DOUBLE, DOUBLE));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RoundVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(ROUND_FUNC_NAME,
        std::vector<common::DataTypeID>{DOUBLE, INT64}, DOUBLE,
        BinaryExecFunction<double_t, int64_t, double_t, operation::Round>));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
BitwiseXorVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        getBinaryDefinition<operation::BitwiseXor, int64_t>(BITWISE_XOR_FUNC_NAME, INT64, INT64));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
BitwiseAndVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        getBinaryDefinition<operation::BitwiseAnd, int64_t>(BITWISE_AND_FUNC_NAME, INT64, INT64));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> BitwiseOrVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(
        getBinaryDefinition<operation::BitwiseOr, int64_t>(BITWISE_OR_FUNC_NAME, INT64, INT64));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
BitShiftLeftVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getBinaryDefinition<operation::BitShiftLeft, int64_t>(
        BITSHIFT_LEFT_FUNC_NAME, INT64, INT64));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
BitShiftRightVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(getBinaryDefinition<operation::BitShiftRight, int64_t>(
        BITSHIFT_RIGHT_FUNC_NAME, INT64, INT64));
    return result;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> PiVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> result;
    result.push_back(make_unique<VectorOperationDefinition>(PI_FUNC_NAME, std::vector<DataTypeID>{},
        DOUBLE, ConstExecFunction<double_t, operation::Pi>));
    return result;
}

} // namespace function
} // namespace kuzu
