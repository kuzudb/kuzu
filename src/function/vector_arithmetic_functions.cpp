#include "function/arithmetic/vector_arithmetic_functions.h"

#include "common/types/date_t.h"
#include "common/types/interval_t.h"
#include "common/types/timestamp_t.h"
#include "function/arithmetic/arithmetic_functions.h"
#include "function/arithmetic/multiply.h"
#include "function/list/functions/list_concat_function.h"
#include "function/list/vector_list_functions.h"
#include "function/string/functions/concat_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set AddFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(ArithmeticFunction::getBinaryFunction<Add>(ADD_FUNC_NAME, typeID));
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
        std::vector<common::LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryStringExecFunction<common::ku_string_t, common::ku_string_t,
            common::ku_string_t, Concat>));
    // interval + interval → interval
    result.push_back(ArithmeticFunction::getBinaryFunction<Add, interval_t, interval_t>(
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
        result.push_back(
            ArithmeticFunction::getBinaryFunction<Subtract>(SUBTRACT_FUNC_NAME, typeID));
    }
    // date - date → int64
    result.push_back(ArithmeticFunction::getBinaryFunction<Subtract, date_t, int64_t>(
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
    result.push_back(ArithmeticFunction::getBinaryFunction<Subtract, timestamp_t, interval_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL));
    // timestamp - interval → timestamp
    result.push_back(make_unique<ScalarFunction>(SUBTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP,
        ScalarFunction::BinaryExecFunction<timestamp_t, interval_t, timestamp_t, Subtract>));
    // interval - interval → interval
    result.push_back(ArithmeticFunction::getBinaryFunction<Subtract, interval_t, interval_t>(
        SUBTRACT_FUNC_NAME, LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
    return result;
}

function_set MultiplyFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(
            ArithmeticFunction::getBinaryFunction<Multiply>(MULTIPLY_FUNC_NAME, typeID));
    }
    return result;
}

function_set DivideFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(ArithmeticFunction::getBinaryFunction<Divide>(DIVIDE_FUNC_NAME, typeID));
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
        result.push_back(ArithmeticFunction::getBinaryFunction<Modulo>(MODULO_FUNC_NAME, typeID));
    }
    return result;
}

function_set PowerFunction::getFunctionSet() {
    function_set result;
    // double_t ^ double_t -> double_t
    result.push_back(ArithmeticFunction::getBinaryFunction<Power, double_t>(
        POWER_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set NegateFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(ArithmeticFunction::getUnaryFunction<Negate>(NEGATE_FUNC_NAME, typeID));
    }
    return result;
}

function_set AbsFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(ArithmeticFunction::getUnaryFunction<Abs>(ABS_FUNC_NAME, typeID));
    }
    return result;
}

function_set FloorFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(ArithmeticFunction::getUnaryFunction<Floor>(FLOOR_FUNC_NAME, typeID));
    }
    return result;
}

function_set CeilFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(ArithmeticFunction::getUnaryFunction<Ceil>(CEIL_FUNC_NAME, typeID));
    }
    return result;
}

function_set SinFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Sin, double_t>(
        SIN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CosFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Cos, double_t>(
        COS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set TanFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Tan, double_t>(
        TAN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CotFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Cot, double_t>(
        COT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AsinFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Asin, double_t>(
        ASIN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AcosFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Acos, double_t>(
        ACOS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AtanFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Atan, double_t>(
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
    result.push_back(ArithmeticFunction::getUnaryFunction<Sqrt, double_t>(
        SQRT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CbrtFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Cbrt, double_t>(
        CBRT_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set GammaFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Gamma, double_t>(
        GAMMA_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LgammaFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Lgamma, double_t>(
        LGAMMA_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LnFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Ln, double_t>(
        LN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LogFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Log, double_t>(
        LOG_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set Log2Function::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Log2, double_t>(
        LOG2_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set DegreesFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Degrees, double_t>(
        DEGREES_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set RadiansFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Radians, double_t>(
        RADIANS_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set EvenFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Even, double_t>(
        EVEN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set SignFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getUnaryFunction<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    result.push_back(ArithmeticFunction::getUnaryFunction<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::INT64));
    result.push_back(ArithmeticFunction::getUnaryFunction<Sign, int64_t>(
        SIGN_FUNC_NAME, LogicalTypeID::FLOAT, LogicalTypeID::INT64));
    return result;
}

function_set Atan2Function::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getBinaryFunction<Atan2, double_t>(
        ATAN2_FUNC_NAME, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set RoundFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(ROUND_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::DOUBLE, LogicalTypeID::INT64},
        LogicalTypeID::DOUBLE,
        ScalarFunction::BinaryExecFunction<double_t, int64_t, double_t, Round>));
    return result;
}

function_set BitwiseXorFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getBinaryFunction<BitwiseXor, int64_t>(
        BITWISE_XOR_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitwiseAndFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getBinaryFunction<BitwiseAnd, int64_t>(
        BITWISE_AND_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitwiseOrFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getBinaryFunction<BitwiseOr, int64_t>(
        BITWISE_OR_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitShiftLeftFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getBinaryFunction<BitShiftLeft, int64_t>(
        BITSHIFT_LEFT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitShiftRightFunction::getFunctionSet() {
    function_set result;
    result.push_back(ArithmeticFunction::getBinaryFunction<BitShiftRight, int64_t>(
        BITSHIFT_RIGHT_FUNC_NAME, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set PiFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(PI_FUNC_NAME, std::vector<LogicalTypeID>{},
        LogicalTypeID::DOUBLE, ScalarFunction::ConstExecFunction<double_t, Pi>));
    return result;
}

} // namespace function
} // namespace kuzu
