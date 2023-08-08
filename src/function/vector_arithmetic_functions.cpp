#include "function/arithmetic/vector_arithmetic_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_function_definitions AddVectorFunction::getDefinitions() {
    vector_function_definitions result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<Add>(ADD_FUNC_NAME, typeID));
    }
    // interval + interval → interval
    result.push_back(getBinaryDefinition<Add, interval_t, interval_t>(
        ADD_FUNC_NAME, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
    // date + int → date
    result.push_back(make_unique<VectorFunctionDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        BinaryExecFunction<date_t, int64_t, date_t, Add>));
    // int + date → date
    result.push_back(make_unique<VectorFunctionDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        BinaryExecFunction<int64_t, date_t, date_t, Add>));
    // date + interval → date
    result.push_back(make_unique<VectorFunctionDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
        LogicalTypeID::DATE, BinaryExecFunction<date_t, interval_t, date_t, Add>));
    // interval + date → date
    result.push_back(make_unique<VectorFunctionDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::DATE},
        LogicalTypeID::DATE, BinaryExecFunction<interval_t, date_t, date_t, Add>));
    // timestamp + interval → timestamp
    result.push_back(make_unique<VectorFunctionDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP, BinaryExecFunction<timestamp_t, interval_t, timestamp_t, Add>));
    // interval + timestamp → timestamp
    result.push_back(make_unique<VectorFunctionDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP, BinaryExecFunction<interval_t, timestamp_t, timestamp_t, Add>));
    return result;
}

vector_function_definitions SubtractVectorFunction::getDefinitions() {
    vector_function_definitions result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<Subtract>(SUBTRACT_FUNC_NAME, typeID));
    }
    // date - date → int64
    result.push_back(getBinaryDefinition<Subtract, date_t, int64_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::DATE, LogicalTypeID::INT64));
    // date - integer → date
    result.push_back(make_unique<VectorFunctionDefinition>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        BinaryExecFunction<date_t, int64_t, date_t, Subtract>));
    // date - interval → date
    result.push_back(make_unique<VectorFunctionDefinition>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
        LogicalTypeID::DATE, BinaryExecFunction<date_t, interval_t, date_t, Subtract>));
    // timestamp - timestamp → interval
    result.push_back(getBinaryDefinition<Subtract, timestamp_t, interval_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL));
    // timestamp - interval → timestamp
    result.push_back(make_unique<VectorFunctionDefinition>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<timestamp_t, interval_t, timestamp_t, Subtract>));
    // interval - interval → interval
    result.push_back(getBinaryDefinition<Subtract, interval_t, interval_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
    return result;
}

vector_function_definitions MultiplyVectorFunction::getDefinitions() {
    vector_function_definitions result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<Multiply>(MULTIPLY_FUNC_NAME, typeID));
    }
    return result;
}

vector_function_definitions DivideVectorFunction::getDefinitions() {
    vector_function_definitions result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<Divide>(DIVIDE_FUNC_NAME, typeID));
    }
    // interval / int → interval
    result.push_back(make_unique<VectorFunctionDefinition>(DIVIDE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::INT64},
        LogicalTypeID::INTERVAL, BinaryExecFunction<interval_t, int64_t, interval_t, Divide>));
    return result;
}

vector_function_definitions ModuloVectorFunction::getDefinitions() {
    vector_function_definitions result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<Modulo>(MODULO_FUNC_NAME, typeID));
    }
    return result;
}

vector_function_definitions PowerVectorFunction::getDefinitions() {
    vector_function_definitions result;
    // double_t ^ double_t -> double_t
    result.push_back(getBinaryDefinition<Power, double_t>(
        POWER_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions NegateVectorFunction::getDefinitions() {
    vector_function_definitions result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<Negate>(NEGATE_FUNC_NAME, typeID));
    }
    return result;
}

vector_function_definitions AbsVectorFunction::getDefinitions() {
    vector_function_definitions result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<Abs>(ABS_FUNC_NAME, typeID));
    }
    return result;
}

vector_function_definitions FloorVectorFunction::getDefinitions() {
    vector_function_definitions result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<Floor>(FLOOR_FUNC_NAME, typeID));
    }
    return result;
}

vector_function_definitions CeilVectorFunction::getDefinitions() {
    vector_function_definitions result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<Ceil>(CEIL_FUNC_NAME, typeID));
    }
    return result;
}

vector_function_definitions SinVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Sin, double_t>(
        SIN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions CosVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Cos, double_t>(
        COS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions TanVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Tan, double_t>(
        TAN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions CotVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Cot, double_t>(
        COT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions AsinVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Asin, double_t>(
        ASIN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions AcosVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Acos, double_t>(
        ACOS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions AtanVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Atan, double_t>(
        ATAN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions FactorialVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(FACTORIAL_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64}, LogicalTypeID::INT64,
        UnaryExecFunction<int64_t, int64_t, Factorial>));
    return result;
}

vector_function_definitions SqrtVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Sqrt, double_t>(
        SQRT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions CbrtVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Cbrt, double_t>(
        CBRT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions GammaVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Gamma, double_t>(
        GAMMA_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions LgammaVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Lgamma, double_t>(
        LGAMMA_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions LnVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Ln, double_t>(
        LN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions LogVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Log, double_t>(
        LOG_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions Log2VectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Log2, double_t>(
        LOG2_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions DegreesVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Degrees, double_t>(
        DEGREES_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions RadiansVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Radians, double_t>(
        RADIANS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions EvenVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Even, double_t>(
        EVEN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions SignVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getUnaryDefinition<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    result.push_back(getUnaryDefinition<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::INT64));
    result.push_back(getUnaryDefinition<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::FLOAT, LogicalTypeID::INT64));
    return result;
}

vector_function_definitions Atan2VectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getBinaryDefinition<Atan2, double_t>(
        ATAN2_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_function_definitions RoundVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(ROUND_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DOUBLE, LogicalTypeID::INT64},
        LogicalTypeID::DOUBLE, BinaryExecFunction<double_t, int64_t, double_t, Round>));
    return result;
}

vector_function_definitions BitwiseXorVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getBinaryDefinition<BitwiseXor, int64_t>(
        BITWISE_XOR_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

vector_function_definitions BitwiseAndVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getBinaryDefinition<BitwiseAnd, int64_t>(
        BITWISE_AND_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

vector_function_definitions BitwiseOrVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getBinaryDefinition<BitwiseOr, int64_t>(
        BITWISE_OR_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

vector_function_definitions BitShiftLeftVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getBinaryDefinition<BitShiftLeft, int64_t>(
        BITSHIFT_LEFT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

vector_function_definitions BitShiftRightVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(getBinaryDefinition<BitShiftRight, int64_t>(
        BITSHIFT_RIGHT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

vector_function_definitions PiVectorFunction::getDefinitions() {
    vector_function_definitions result;
    result.push_back(make_unique<VectorFunctionDefinition>(PI_FUNC_NAME,
        std::vector<LogicalTypeID>{}, LogicalTypeID::DOUBLE, ConstExecFunction<double_t, Pi>));
    return result;
}

} // namespace function
} // namespace kuzu
