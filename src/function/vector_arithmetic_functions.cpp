#include "function/arithmetic/vector_arithmetic_functions.h"

#include "common/types/date_t.h"
#include "common/types/interval_t.h"
#include "common/types/timestamp_t.h"
#include "function/arithmetic/abs.h"
#include "function/arithmetic/add.h"
#include "function/arithmetic/arithmetic_functions.h"
#include "function/arithmetic/divide.h"
#include "function/arithmetic/modulo.h"
#include "function/arithmetic/multiply.h"
#include "function/arithmetic/negate.h"
#include "function/arithmetic/subtract.h"
#include "function/list/functions/list_concat_function.h"
#include "function/list/vector_list_functions.h"
#include "function/string/functions/concat_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

template<typename FUNC>
static void getUnaryExecFunc(LogicalTypeID operandTypeID, scalar_exec_func& func) {
    switch (operandTypeID) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        func = ScalarFunction::UnaryExecFunction<int64_t, int64_t, FUNC>;
    } break;
    case LogicalTypeID::INT32: {
        func = ScalarFunction::UnaryExecFunction<int32_t, int32_t, FUNC>;
    } break;
    case LogicalTypeID::INT16: {
        func = ScalarFunction::UnaryExecFunction<int16_t, int16_t, FUNC>;
    } break;
    case LogicalTypeID::INT8: {
        func = ScalarFunction::UnaryExecFunction<int8_t, int8_t, FUNC>;
    } break;
    case LogicalTypeID::UINT64: {
        func = ScalarFunction::UnaryExecFunction<uint64_t, uint64_t, FUNC>;
    } break;
    case LogicalTypeID::UINT32: {
        func = ScalarFunction::UnaryExecFunction<uint32_t, uint32_t, FUNC>;
    } break;
    case LogicalTypeID::UINT16: {
        func = ScalarFunction::UnaryExecFunction<uint16_t, uint16_t, FUNC>;
    } break;
    case LogicalTypeID::UINT8: {
        func = ScalarFunction::UnaryExecFunction<uint8_t, uint8_t, FUNC>;
    } break;
    case LogicalTypeID::INT128: {
        func = ScalarFunction::UnaryExecFunction<int128_t, int128_t, FUNC>;
    } break;
    case LogicalTypeID::DOUBLE: {
        func = ScalarFunction::UnaryExecFunction<double, double, FUNC>;
    } break;
    case LogicalTypeID::FLOAT: {
        func = ScalarFunction::UnaryExecFunction<float, float, FUNC>;
    } break;
    default:
        KU_UNREACHABLE;
    }
}

template<typename FUNC>
static void getBinaryExecFunc(LogicalTypeID operandTypeID, scalar_exec_func& func) {
    switch (operandTypeID) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::INT64: {
        func = ScalarFunction::BinaryExecFunction<int64_t, int64_t, int64_t, FUNC>;
    } break;
    case LogicalTypeID::INT32: {
        func = ScalarFunction::BinaryExecFunction<int32_t, int32_t, int32_t, FUNC>;
    } break;
    case LogicalTypeID::INT16: {
        func = ScalarFunction::BinaryExecFunction<int16_t, int16_t, int16_t, FUNC>;
    } break;
    case LogicalTypeID::INT8: {
        func = ScalarFunction::BinaryExecFunction<int8_t, int8_t, int8_t, FUNC>;
    } break;
    case LogicalTypeID::UINT64: {
        func = ScalarFunction::BinaryExecFunction<uint64_t, uint64_t, uint64_t, FUNC>;
    } break;
    case LogicalTypeID::UINT32: {
        func = ScalarFunction::BinaryExecFunction<uint32_t, uint32_t, uint32_t, FUNC>;
    } break;
    case LogicalTypeID::UINT16: {
        func = ScalarFunction::BinaryExecFunction<uint16_t, uint16_t, uint16_t, FUNC>;
    } break;
    case LogicalTypeID::UINT8: {
        func = ScalarFunction::BinaryExecFunction<uint8_t, uint8_t, uint8_t, FUNC>;
    } break;
    case LogicalTypeID::INT128: {
        func = ScalarFunction::BinaryExecFunction<int128_t, int128_t, int128_t, FUNC>;
    } break;
    case LogicalTypeID::DOUBLE: {
        func = ScalarFunction::BinaryExecFunction<double, double, double, FUNC>;
    } break;
    case LogicalTypeID::FLOAT: {
        func = ScalarFunction::BinaryExecFunction<float, float, float, FUNC>;
    } break;
    default:
        KU_UNREACHABLE;
    }
}

template<typename FUNC>
static std::unique_ptr<ScalarFunction> getUnaryFunction(
    std::string name, LogicalTypeID operandTypeID) {
    function::scalar_exec_func execFunc;
    getUnaryExecFunc<FUNC>(operandTypeID, execFunc);
    return std::make_unique<ScalarFunction>(
        std::move(name), std::vector<LogicalTypeID>{operandTypeID}, operandTypeID, execFunc);
}

template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
static std::unique_ptr<ScalarFunction> getUnaryFunction(
    std::string name, LogicalTypeID operandTypeID, LogicalTypeID resultTypeID) {
    return std::make_unique<ScalarFunction>(std::move(name),
        std::vector<LogicalTypeID>{operandTypeID}, resultTypeID,
        ScalarFunction::UnaryExecFunction<OPERAND_TYPE, RETURN_TYPE, FUNC>);
}

template<typename FUNC>
static inline std::unique_ptr<ScalarFunction> getBinaryFunction(
    std::string name, LogicalTypeID operandTypeID) {
    function::scalar_exec_func execFunc;
    getBinaryExecFunc<FUNC>(operandTypeID, execFunc);
    return std::make_unique<ScalarFunction>(std::move(name),
        std::vector<LogicalTypeID>{operandTypeID, operandTypeID}, operandTypeID, execFunc);
}

template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
static inline std::unique_ptr<ScalarFunction> getBinaryFunction(
    std::string name, LogicalTypeID operandTypeID, LogicalTypeID resultTypeID) {
    return std::make_unique<ScalarFunction>(std::move(name),
        std::vector<LogicalTypeID>{operandTypeID, operandTypeID}, resultTypeID,
        ScalarFunction::BinaryExecFunction<OPERAND_TYPE, OPERAND_TYPE, RETURN_TYPE, FUNC>);
}

function_set AddFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryFunction<Add>(ADD_FUNC_NAME, typeID));
    }
    // list + list -> list
    result.push_back(std::make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::VAR_LIST, LogicalTypeID::VAR_LIST},
        LogicalTypeID::VAR_LIST,
        ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t, list_entry_t,
            ListConcat>,
        nullptr, ListConcatFunction::bindFunc, false /* isVarlength*/));
    // string + string -> string
    result.push_back(std::make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, Concat>));
    // interval + interval → interval
    result.push_back(getBinaryFunction<Add, interval_t, interval_t>(
        ADD_FUNC_NAME, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
    // date + int → date
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        ScalarFunction::BinaryExecFunction<date_t, int64_t, date_t, Add>));
    // int + date → date
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        ScalarFunction::BinaryExecFunction<int64_t, date_t, date_t, Add>));
    // date + interval → date
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
        LogicalTypeID::DATE, ScalarFunction::BinaryExecFunction<date_t, interval_t, date_t, Add>));
    // interval + date → date
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::DATE},
        LogicalTypeID::DATE, ScalarFunction::BinaryExecFunction<interval_t, date_t, date_t, Add>));
    // timestamp + interval → timestamp
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP,
        ScalarFunction::BinaryExecFunction<timestamp_t, interval_t, timestamp_t, Add>));
    // interval + timestamp → timestamp
    result.push_back(make_unique<ScalarFunction>(ADD_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP,
        ScalarFunction::BinaryExecFunction<interval_t, timestamp_t, timestamp_t, Add>));
    return result;
}

function_set SubtractFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryFunction<Subtract>(SUBTRACT_FUNC_NAME, typeID));
    }
    // date - date → int64
    result.push_back(getBinaryFunction<Subtract, date_t, int64_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::DATE, LogicalTypeID::INT64));
    // date - integer → date
    result.push_back(make_unique<ScalarFunction>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        ScalarFunction::BinaryExecFunction<date_t, int64_t, date_t, Subtract>));
    // date - interval → date
    result.push_back(make_unique<ScalarFunction>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
        LogicalTypeID::DATE,
        ScalarFunction::BinaryExecFunction<date_t, interval_t, date_t, Subtract>));
    // timestamp - timestamp → interval
    result.push_back(getBinaryFunction<Subtract, timestamp_t, interval_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL));
    // timestamp - interval → timestamp
    result.push_back(make_unique<ScalarFunction>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP,
        ScalarFunction::BinaryExecFunction<timestamp_t, interval_t, timestamp_t, Subtract>));
    // interval - interval → interval
    result.push_back(getBinaryFunction<Subtract, interval_t, interval_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
    return result;
}

function_set MultiplyFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryFunction<Multiply>(MULTIPLY_FUNC_NAME, typeID));
    }
    return result;
}

function_set DivideFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryFunction<Divide>(DIVIDE_FUNC_NAME, typeID));
    }
    // interval / int → interval
    result.push_back(make_unique<ScalarFunction>(DIVIDE_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::INT64},
        LogicalTypeID::INTERVAL,
        ScalarFunction::BinaryExecFunction<interval_t, int64_t, interval_t, Divide>));
    return result;
}

function_set ModuloFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryFunction<Modulo>(MODULO_FUNC_NAME, typeID));
    }
    return result;
}

function_set PowerFunction::getFunctionSet() {
    function_set result;
    // double ^ double -> double
    result.push_back(getBinaryFunction<Power, double>(
        POWER_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set NegateFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryFunction<Negate>(NEGATE_FUNC_NAME, typeID));
    }
    return result;
}

function_set AbsFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryFunction<Abs>(name, typeID));
    }
    return result;
}

function_set FloorFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryFunction<Floor>(FLOOR_FUNC_NAME, typeID));
    }
    return result;
}

function_set CeilFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryFunction<Ceil>(CEIL_FUNC_NAME, typeID));
    }
    return result;
}

function_set SinFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Sin, double>(SIN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CosFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Cos, double>(COS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set TanFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Tan, double>(TAN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CotFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Cot, double>(COT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AsinFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Asin, double>(
        ASIN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AcosFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Acos, double>(
        ACOS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AtanFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Atan, double>(
        ATAN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set FactorialFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(FACTORIAL_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64}, LogicalTypeID::INT64,
        ScalarFunction::UnaryExecFunction<int64_t, int64_t, Factorial>));
    return result;
}

function_set SqrtFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Sqrt, double>(
        SQRT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CbrtFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Cbrt, double>(
        CBRT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set GammaFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Gamma, double>(
        GAMMA_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LgammaFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Lgamma, double>(
        LGAMMA_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LnFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Ln, double>(LN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LogFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Log, double>(LOG_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set Log2Function::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Log2, double>(
        LOG2_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set DegreesFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Degrees, double>(
        DEGREES_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set RadiansFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Radians, double>(
        RADIANS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set EvenFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Even, double>(
        EVEN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set SignFunction::getFunctionSet() {
    function_set result;
    result.push_back(getUnaryFunction<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    result.push_back(getUnaryFunction<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::INT64));
    result.push_back(getUnaryFunction<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::FLOAT, LogicalTypeID::INT64));
    return result;
}

function_set Atan2Function::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryFunction<Atan2, double>(
        ATAN2_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set RoundFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(ROUND_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DOUBLE, LogicalTypeID::INT64},
        LogicalTypeID::DOUBLE, ScalarFunction::BinaryExecFunction<double, int64_t, double, Round>));
    return result;
}

function_set BitwiseXorFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryFunction<BitwiseXor, int64_t>(
        BITWISE_XOR_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitwiseAndFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryFunction<BitwiseAnd, int64_t>(
        BITWISE_AND_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitwiseOrFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryFunction<BitwiseOr, int64_t>(
        BITWISE_OR_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitShiftLeftFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryFunction<BitShiftLeft, int64_t>(
        BITSHIFT_LEFT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitShiftRightFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryFunction<BitShiftRight, int64_t>(
        BITSHIFT_RIGHT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set PiFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(PI_FUNC_NAME, std::vector<LogicalTypeID>{},
        LogicalTypeID::DOUBLE, ScalarFunction::ConstExecFunction<double, Pi>));
    return result;
}

} // namespace function
} // namespace kuzu
