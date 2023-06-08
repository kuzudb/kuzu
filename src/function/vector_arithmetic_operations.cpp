#include "function/arithmetic/vector_arithmetic_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_operation_definitions AddVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Add>(ADD_FUNC_NAME, typeID));
    }
    // interval + interval → interval
    result.push_back(getBinaryDefinition<operation::Add, interval_t, interval_t>(
        ADD_FUNC_NAME, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
    // date + int → date
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        BinaryExecFunction<date_t, int64_t, date_t, operation::Add>));
    // int + date → date
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        BinaryExecFunction<int64_t, date_t, date_t, operation::Add>));
    // date + interval → date
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
        LogicalTypeID::DATE, BinaryExecFunction<date_t, interval_t, date_t, operation::Add>));
    // interval + date → date
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::DATE},
        LogicalTypeID::DATE, BinaryExecFunction<interval_t, date_t, date_t, operation::Add>));
    // timestamp + interval → timestamp
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Add>));
    // interval + timestamp → timestamp
    result.push_back(make_unique<VectorOperationDefinition>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<interval_t, timestamp_t, timestamp_t, operation::Add>));
    return result;
}

vector_operation_definitions SubtractVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Subtract>(SUBTRACT_FUNC_NAME, typeID));
    }
    // date - date → int64
    result.push_back(getBinaryDefinition<operation::Subtract, date_t, int64_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::DATE, LogicalTypeID::INT64));
    // date - integer → date
    result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        BinaryExecFunction<date_t, int64_t, date_t, operation::Subtract>));
    // date - interval → date
    result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
        LogicalTypeID::DATE, BinaryExecFunction<date_t, interval_t, date_t, operation::Subtract>));
    // timestamp - timestamp → interval
    result.push_back(getBinaryDefinition<operation::Subtract, timestamp_t, interval_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL));
    // timestamp - interval → timestamp
    result.push_back(make_unique<VectorOperationDefinition>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP,
        BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Subtract>));
    // interval - interval → interval
    result.push_back(getBinaryDefinition<operation::Subtract, interval_t, interval_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
    return result;
}

vector_operation_definitions MultiplyVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Multiply>(MULTIPLY_FUNC_NAME, typeID));
    }
    return result;
}

vector_operation_definitions DivideVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Divide>(DIVIDE_FUNC_NAME, typeID));
    }
    // interval / int → interval
    result.push_back(make_unique<VectorOperationDefinition>(DIVIDE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::INT64},
        LogicalTypeID::INTERVAL,
        BinaryExecFunction<interval_t, int64_t, interval_t, operation::Divide>));
    return result;
}

vector_operation_definitions ModuloVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryDefinition<operation::Modulo>(MODULO_FUNC_NAME, typeID));
    }
    return result;
}

vector_operation_definitions PowerVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    // double_t ^ double_t -> double_t
    result.push_back(getBinaryDefinition<operation::Power, double_t>(
        POWER_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions NegateVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Negate>(NEGATE_FUNC_NAME, typeID));
    }
    return result;
}

vector_operation_definitions AbsVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Abs>(ABS_FUNC_NAME, typeID));
    }
    return result;
}

vector_operation_definitions FloorVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Floor>(FLOOR_FUNC_NAME, typeID));
    }
    return result;
}

vector_operation_definitions CeilVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryDefinition<operation::Ceil>(CEIL_FUNC_NAME, typeID));
    }
    return result;
}

vector_operation_definitions SinVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Sin, double_t>(
        SIN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions CosVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Cos, double_t>(
        COS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions TanVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Tan, double_t>(
        TAN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions CotVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Cot, double_t>(
        COT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions AsinVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Asin, double_t>(
        ASIN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions AcosVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Acos, double_t>(
        ACOS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions AtanVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Atan, double_t>(
        ATAN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions FactorialVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(make_unique<VectorOperationDefinition>(FACTORIAL_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64}, LogicalTypeID::INT64,
        UnaryExecFunction<int64_t, int64_t, operation::Factorial>));
    return result;
}

vector_operation_definitions SqrtVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Sqrt, double_t>(
        SQRT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions CbrtVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Cbrt, double_t>(
        CBRT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions GammaVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Gamma, double_t>(
        GAMMA_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions LgammaVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Lgamma, double_t>(
        LGAMMA_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions LnVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Ln, double_t>(
        LN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions LogVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Log, double_t>(
        LOG_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions Log2VectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Log2, double_t>(
        LOG2_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions DegreesVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Degrees, double_t>(
        DEGREES_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions RadiansVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Radians, double_t>(
        RADIANS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions EvenVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Even, double_t>(
        EVEN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions SignVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getUnaryDefinition<operation::Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    result.push_back(getUnaryDefinition<operation::Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::INT64));
    result.push_back(getUnaryDefinition<operation::Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::FLOAT, LogicalTypeID::INT64));
    return result;
}

vector_operation_definitions Atan2VectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getBinaryDefinition<operation::Atan2, double_t>(
        ATAN2_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

vector_operation_definitions RoundVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(make_unique<VectorOperationDefinition>(ROUND_FUNC_NAME,
        std::vector<common::LogicalTypeID>{LogicalTypeID::DOUBLE, LogicalTypeID::INT64},
        LogicalTypeID::DOUBLE, BinaryExecFunction<double_t, int64_t, double_t, operation::Round>));
    return result;
}

vector_operation_definitions BitwiseXorVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getBinaryDefinition<operation::BitwiseXor, int64_t>(
        BITWISE_XOR_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

vector_operation_definitions BitwiseAndVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getBinaryDefinition<operation::BitwiseAnd, int64_t>(
        BITWISE_AND_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

vector_operation_definitions BitwiseOrVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getBinaryDefinition<operation::BitwiseOr, int64_t>(
        BITWISE_OR_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

vector_operation_definitions BitShiftLeftVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getBinaryDefinition<operation::BitShiftLeft, int64_t>(
        BITSHIFT_LEFT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

vector_operation_definitions BitShiftRightVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(getBinaryDefinition<operation::BitShiftRight, int64_t>(
        BITSHIFT_RIGHT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

vector_operation_definitions PiVectorOperation::getDefinitions() {
    vector_operation_definitions result;
    result.push_back(
        make_unique<VectorOperationDefinition>(PI_FUNC_NAME, std::vector<LogicalTypeID>{},
            LogicalTypeID::DOUBLE, ConstExecFunction<double_t, operation::Pi>));
    return result;
}

} // namespace function
} // namespace kuzu
