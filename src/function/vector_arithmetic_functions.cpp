#include "function/arithmetic/vector_arithmetic_functions.h"

#include "common/exception/overflow.h"
#include "common/exception/runtime.h"
#include "common/type_utils.h"
#include "common/types/date_t.h"
#include "common/types/int128_t.h"
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
#include "function/cast/functions/numeric_limits.h"
#include "function/list/functions/list_concat_function.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"
#include "function/string/vector_string_functions.h"

using namespace kuzu::common;
using std::max;
using std::min;

namespace kuzu {
namespace function {

struct DecimalFunction {

    static std::unique_ptr<FunctionBindData> bindAddFunc(const binder::expression_vector& arguments,
        Function*);

    static std::unique_ptr<FunctionBindData> bindSubtractFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindMultiplyFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindDivideFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindModuloFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindNegateFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindAbsFunc(const binder::expression_vector& arguments,
        Function*);

    static std::unique_ptr<FunctionBindData> bindFloorFunc(
        const binder::expression_vector& arguments, Function*);

    static std::unique_ptr<FunctionBindData> bindCeilFunc(
        const binder::expression_vector& arguments, Function*);
};

template<typename FUNC>
static void getUnaryExecFunc(LogicalTypeID operandTypeID, scalar_func_exec_t& func) {
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
static void getBinaryExecFunc(LogicalTypeID operandTypeID, scalar_func_exec_t& func) {
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
static std::unique_ptr<ScalarFunction> getUnaryFunction(std::string name,
    LogicalTypeID operandTypeID) {
    function::scalar_func_exec_t execFunc;
    getUnaryExecFunc<FUNC>(operandTypeID, execFunc);
    return std::make_unique<ScalarFunction>(std::move(name),
        std::vector<LogicalTypeID>{operandTypeID}, operandTypeID, execFunc);
}

template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
static std::unique_ptr<ScalarFunction> getUnaryFunction(std::string name,
    LogicalTypeID operandTypeID, LogicalTypeID resultTypeID) {
    return std::make_unique<ScalarFunction>(std::move(name),
        std::vector<LogicalTypeID>{operandTypeID}, resultTypeID,
        ScalarFunction::UnaryExecFunction<OPERAND_TYPE, RETURN_TYPE, FUNC>);
}

template<typename FUNC>
static inline std::unique_ptr<ScalarFunction> getBinaryFunction(std::string name,
    LogicalTypeID operandTypeID) {
    function::scalar_func_exec_t execFunc;
    getBinaryExecFunc<FUNC>(operandTypeID, execFunc);
    return std::make_unique<ScalarFunction>(std::move(name),
        std::vector<LogicalTypeID>{operandTypeID, operandTypeID}, operandTypeID, execFunc);
}

template<typename FUNC, typename OPERAND_TYPE, typename RETURN_TYPE = OPERAND_TYPE>
static inline std::unique_ptr<ScalarFunction> getBinaryFunction(std::string name,
    LogicalTypeID operandTypeID, LogicalTypeID resultTypeID) {
    return std::make_unique<ScalarFunction>(std::move(name),
        std::vector<LogicalTypeID>{operandTypeID, operandTypeID}, resultTypeID,
        ScalarFunction::BinaryExecFunction<OPERAND_TYPE, OPERAND_TYPE, RETURN_TYPE, FUNC>);
}

function_set AddFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryFunction<Add>(name, typeID));
    }

    // decimal + decimal -> decimal
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL, LogicalTypeID::DECIMAL},
        LogicalTypeID::DECIMAL, nullptr, nullptr, DecimalFunction::bindAddFunc));
    // list + list -> list
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::LIST,
        ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t, list_entry_t,
            ListConcat>,
        nullptr, ListConcatFunction::bindFunc));
    // string + string -> string
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING, ConcatFunction::execFunc));
    // interval + interval → interval
    result.push_back(getBinaryFunction<Add, interval_t, interval_t>(name, LogicalTypeID::INTERVAL,
        LogicalTypeID::INTERVAL));
    // date + int → date
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        ScalarFunction::BinaryExecFunction<date_t, int64_t, date_t, Add>));
    // int + date → date
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::INT64, LogicalTypeID::DATE}, LogicalTypeID::DATE,
        ScalarFunction::BinaryExecFunction<int64_t, date_t, date_t, Add>));
    // date + interval → date
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
        LogicalTypeID::DATE, ScalarFunction::BinaryExecFunction<date_t, interval_t, date_t, Add>));
    // interval + date → date
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::DATE},
        LogicalTypeID::DATE, ScalarFunction::BinaryExecFunction<interval_t, date_t, date_t, Add>));
    // timestamp + interval → timestamp
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP,
        ScalarFunction::BinaryExecFunction<timestamp_t, interval_t, timestamp_t, Add>));
    // interval + timestamp → timestamp
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::TIMESTAMP},
        LogicalTypeID::TIMESTAMP,
        ScalarFunction::BinaryExecFunction<interval_t, timestamp_t, timestamp_t, Add>));
    return result;
}

function_set SubtractFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryFunction<Subtract>(name, typeID));
    }
    // decimal - decimal -> decimal
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL, LogicalTypeID::DECIMAL},
        LogicalTypeID::DECIMAL, nullptr, nullptr, DecimalFunction::bindSubtractFunc));
    // date - date → int64
    result.push_back(getBinaryFunction<Subtract, date_t, int64_t>(name, LogicalTypeID::DATE,
        LogicalTypeID::INT64));
    // date - integer → date
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INT64}, LogicalTypeID::DATE,
        ScalarFunction::BinaryExecFunction<date_t, int64_t, date_t, Subtract>));
    // date - interval → date
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::DATE, LogicalTypeID::INTERVAL},
        LogicalTypeID::DATE,
        ScalarFunction::BinaryExecFunction<date_t, interval_t, date_t, Subtract>));
    // timestamp - timestamp → interval
    result.push_back(getBinaryFunction<Subtract, timestamp_t, interval_t>(name,
        LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL));
    // timestamp - interval → timestamp
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::TIMESTAMP, LogicalTypeID::INTERVAL},
        LogicalTypeID::TIMESTAMP,
        ScalarFunction::BinaryExecFunction<timestamp_t, interval_t, timestamp_t, Subtract>));
    // interval - interval → interval
    result.push_back(getBinaryFunction<Subtract, interval_t, interval_t>(name,
        LogicalTypeID::INTERVAL, LogicalTypeID::INTERVAL));
    return result;
}

function_set MultiplyFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryFunction<Multiply>(name, typeID));
    }
    // decimal * decimal -> decimal
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL, LogicalTypeID::DECIMAL},
        LogicalTypeID::DECIMAL, nullptr, nullptr, DecimalFunction::bindMultiplyFunc));
    return result;
}

function_set DivideFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryFunction<Divide>(name, typeID));
    }
    // interval / int → interval
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::INTERVAL, LogicalTypeID::INT64},
        LogicalTypeID::INTERVAL,
        ScalarFunction::BinaryExecFunction<interval_t, int64_t, interval_t, Divide>));
    // decimal / decimal -> decimal
    // drop to double division for now
    // result.push_back(make_unique<ScalarFunction>(name,
    // std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL, LogicalTypeID::DECIMAL},
    // LogicalTypeID::DECIMAL, nullptr, nullptr, DecimalFunction::bindDivideFunc));
    return result;
}

function_set ModuloFunction::getFunctionSet() {
    function_set result;
    for (auto typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getBinaryFunction<Modulo>(name, typeID));
    }
    // decimal % decimal -> decimal
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL, LogicalTypeID::DECIMAL},
        LogicalTypeID::DECIMAL, nullptr, nullptr, DecimalFunction::bindModuloFunc));
    return result;
}

function_set PowerFunction::getFunctionSet() {
    function_set result;
    // double ^ double -> double
    result.push_back(
        getBinaryFunction<Power, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set NegateFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryFunction<Negate>(name, typeID));
    }
    // floor(decimal) -> decimal
    result.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL},
            LogicalTypeID::DECIMAL, nullptr, nullptr, DecimalFunction::bindNegateFunc));
    return result;
}

function_set AbsFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryFunction<Abs>(name, typeID));
    }
    result.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL},
            LogicalTypeID::DECIMAL, nullptr, nullptr, DecimalFunction::bindAbsFunc));
    return result;
}

function_set FloorFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryFunction<Floor>(name, typeID));
    }
    result.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL},
            LogicalTypeID::DECIMAL, nullptr, nullptr, DecimalFunction::bindFloorFunc));
    return result;
}

function_set CeilFunction::getFunctionSet() {
    function_set result;
    for (auto& typeID : LogicalTypeUtils::getNumericalLogicalTypeIDs()) {
        result.push_back(getUnaryFunction<Ceil>(name, typeID));
    }
    result.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::DECIMAL},
            LogicalTypeID::DECIMAL, nullptr, nullptr, DecimalFunction::bindCeilFunc));
    return result;
}

function_set SinFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Sin, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CosFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Cos, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set TanFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Tan, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CotFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Cot, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AsinFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Asin, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AcosFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Acos, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set AtanFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Atan, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set FactorialFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::INT64},
            LogicalTypeID::INT64, ScalarFunction::UnaryExecFunction<int64_t, int64_t, Factorial>));
    return result;
}

function_set SqrtFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Sqrt, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set CbrtFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Cbrt, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set GammaFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Gamma, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LgammaFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Lgamma, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LnFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Ln, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set LogFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Log, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set Log2Function::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Log2, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set DegreesFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Degrees, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set RadiansFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Radians, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set EvenFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Even, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set SignFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getUnaryFunction<Sign, int64_t>(name, LogicalTypeID::INT64, LogicalTypeID::INT64));
    result.push_back(
        getUnaryFunction<Sign, int64_t>(name, LogicalTypeID::DOUBLE, LogicalTypeID::INT64));
    result.push_back(
        getUnaryFunction<Sign, int64_t>(name, LogicalTypeID::FLOAT, LogicalTypeID::INT64));
    return result;
}

function_set Atan2Function::getFunctionSet() {
    function_set result;
    result.push_back(
        getBinaryFunction<Atan2, double>(name, LogicalTypeID::DOUBLE, LogicalTypeID::DOUBLE));
    return result;
}

function_set RoundFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::DOUBLE, LogicalTypeID::INT64},
        LogicalTypeID::DOUBLE, ScalarFunction::BinaryExecFunction<double, int64_t, double, Round>));
    return result;
}

function_set BitwiseXorFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getBinaryFunction<BitwiseXor, int64_t>(name, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitwiseAndFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getBinaryFunction<BitwiseAnd, int64_t>(name, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitwiseOrFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getBinaryFunction<BitwiseOr, int64_t>(name, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitShiftLeftFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        getBinaryFunction<BitShiftLeft, int64_t>(name, LogicalTypeID::INT64, LogicalTypeID::INT64));
    return result;
}

function_set BitShiftRightFunction::getFunctionSet() {
    function_set result;
    result.push_back(getBinaryFunction<BitShiftRight, int64_t>(name, LogicalTypeID::INT64,
        LogicalTypeID::INT64));
    return result;
}

function_set PiFunction::getFunctionSet() {
    function_set result;
    result.push_back(make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{},
        LogicalTypeID::DOUBLE, ScalarFunction::NullaryExecFunction<double, Pi>));
    return result;
}

using param_get_func_t = std::function<std::pair<int, int>(int, int, int, int)>;

// Following param func rules are from
// https://learn.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql
// todo: Figure out which param rules we should use

struct DecimalAdd {
    static constexpr bool matchToOutputLogicalType = true;
    // whether or not the input and output logical types
    // are expected to be equivalent. If so, the bind function
    // should specify that the input be casted to the output type before execution
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result,
        common::ValueVector& resultValueVector) {
        constexpr auto pow10s = pow10Sequence<R>();
        auto precision = DecimalType::getPrecision(resultValueVector.dataType);
        if ((right > 0 && pow10s[precision] - right <= left) ||
            (right < 0 && -pow10s[precision] - right >= left)) {
            throw OverflowException("Decimal Addition result is out of range");
        }
        result = left + right;
    }

    static std::pair<int, int> resultingParams(int p1, int p2, int s1, int s2) {
        auto p = min(DECIMAL_PRECISION_LIMIT, max(s1, s2) + max(p1 - s1, p2 - s2) + 1);
        auto s = min(p, max(s1, s2));
        if (max(p1 - s1, p2 - s2) < min(DECIMAL_PRECISION_LIMIT, p) - s) {
            s = min(p, DECIMAL_PRECISION_LIMIT) - max(p1 - s1, p2 - s2);
        }
        return {p, s};
    }
};

struct DecimalSubtract {
    static constexpr bool matchToOutputLogicalType = true;
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result,
        common::ValueVector& resultValueVector) {
        constexpr auto pow10s = pow10Sequence<R>();
        auto precision = DecimalType::getPrecision(resultValueVector.dataType);
        if ((right > 0 && -pow10s[precision] + right >= left) ||
            (right < 0 && pow10s[precision] + right <= left)) {
            throw OverflowException("Decimal Subtraction result is out of range");
        }
        result = left - right;
    }

    static std::pair<int, int> resultingParams(int p1, int p2, int s1, int s2) {
        auto p = min(DECIMAL_PRECISION_LIMIT, max(s1, s2) + max(p1 - s1, p2 - s2) + 1);
        auto s = min(p, max(s1, s2));
        if (max(p1 - s1, p2 - s2) < min(DECIMAL_PRECISION_LIMIT, p) - s) {
            s = min(p, DECIMAL_PRECISION_LIMIT) - max(p1 - s1, p2 - s2);
        }
        return {p, s};
    }
};

struct DecimalMultiply {
    static constexpr bool matchToOutputLogicalType = false;
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result,
        common::ValueVector& resultValueVector) {
        constexpr auto pow10s = pow10Sequence<R>();
        auto precision = DecimalType::getPrecision(resultValueVector.dataType);
        result = (R)left * (R)right;
        // no need to divide by any scale given resultingParams and matchToOutput
        if (result <= -pow10s[precision] || result >= pow10s[precision]) {
            [[unlikely]] throw OverflowException("Decimal Multiplication Result is out of range");
        }
    }

    static std::pair<int, int> resultingParams(int p1, int p2, int s1, int s2) {
        if (p1 + p2 + 1 > DECIMAL_PRECISION_LIMIT) {
            throw OverflowException(
                "Resulting precision of decimal multiplication greater than 38");
        }
        auto p = p1 + p2 + 1;
        auto s = s1 + s2;
        return {p, s};
    }
};

struct DecimalDivide {
    static constexpr bool matchToOutputLogicalType = true;
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result,
        common::ValueVector& resultValueVector) {
        constexpr auto pow10s = pow10Sequence<R>();
        auto precision = DecimalType::getPrecision(resultValueVector.dataType);
        auto scale = DecimalType::getScale(resultValueVector.dataType);
        if (right == 0) {
            throw RuntimeException("Divide by zero.");
        }
        if (-pow10s[precision - scale] >= left || pow10s[precision - scale] <= left) {
            throw OverflowException("Overflow encountered when attempting to divide decimals");
            // happens too often; let's just drop to double division for now, which is in line with
            // what DuckDB does right now
        }
        result = (left * pow10s[scale]) / right;
    }

    static std::pair<int, int> resultingParams(int p1, int p2, int s1, int s2) {
        auto p = min(DECIMAL_PRECISION_LIMIT, p1 - s1 + s2 + max(6, s1 + p2 + 1));
        auto s = min(p, max(6, s1 + p2 + 1)); // todo: complete rules
        return {p, s};
    }
};

struct DecimalModulo {
    static constexpr bool matchToOutputLogicalType = true;
    template<typename A, typename B, typename R>
    static inline void operation(A& left, B& right, R& result, common::ValueVector&) {
        if (right == 0) {
            throw RuntimeException("Modulo by zero.");
        }
        result = left % right;
    }

    static std::pair<int, int> resultingParams(int p1, int p2, int s1, int s2) {
        auto p = min(DECIMAL_PRECISION_LIMIT, min(p1 - s1, p2 - s2) + max(s1, s2));
        auto s = min(p, max(s1, s2));
        return {p, s};
    }
};

struct DecimalNegate {
    static constexpr bool matchToOutputLogicalType = true;
    template<typename A, typename R>
    static inline void operation(A& input, R& result, common::ValueVector&, common::ValueVector&) {
        result = -input;
    }

    static std::pair<int, int> resultingParams(int p, int s) { return {p, s}; }
};

struct DecimalAbs {
    static constexpr bool matchToOutputLogicalType = true;
    template<typename A, typename R>
    static inline void operation(A& input, R& result, common::ValueVector&, common::ValueVector&) {
        result = input;
        if (result < 0) {
            result = -result;
        }
    }

    static std::pair<int, int> resultingParams(int p, int s) { return {p, s}; }
};

struct DecimalFloor {
    static constexpr bool matchToOutputLogicalType = false;
    template<typename A, typename R>
    static inline void operation(A& input, R& result, common::ValueVector& inputVector,
        common::ValueVector&) {
        constexpr auto pow10s = pow10Sequence<R>();
        auto scale = DecimalType::getScale(inputVector.dataType);
        if (input < 0) {
            // round to larger absolute value
            result = (R)input -
                     (input % pow10s[scale] == 0 ? 0 : pow10s[scale] + (R)(input % pow10s[scale]));
        } else {
            // round to smaller absolute value
            result = (R)input - (R)(input % pow10s[scale]);
        }
        result = result / pow10s[scale];
    }

    static std::pair<int, int> resultingParams(int p, int) { return {p, 0}; }
};

struct DecimalCeil {
    static constexpr bool matchToOutputLogicalType = false;
    template<typename A, typename R>
    static inline void operation(A& input, R& result, common::ValueVector& inputVector,
        common::ValueVector&) {
        constexpr auto pow10s = pow10Sequence<R>();
        auto scale = DecimalType::getScale(inputVector.dataType);
        if (input < 0) {
            // round to larger absolute value
            result = (R)input - (R)(input % pow10s[scale]);
        } else {
            // round to smaller absolute value
            result = (R)input +
                     (input % pow10s[scale] == 0 ? 0 : pow10s[scale] - (R)(input % pow10s[scale]));
        }
        result = result / pow10s[scale];
    }

    static std::pair<int, int> resultingParams(int p, int) { return {p, 0}; }
};

template<typename FUNC, typename A, typename B>
static void getBinaryExecutionHelperB(const LogicalType& typeR, scalar_func_exec_t& result) {
    // here to assist in getting scalar_func_exec_t for genericBinaryArithmeticFunc
    switch (typeR.getPhysicalType()) {
    case PhysicalTypeID::INT16:
        result = ScalarFunction::BinaryStringExecFunction<A, B, int16_t, FUNC>;
        break;
    case PhysicalTypeID::INT32:
        result = ScalarFunction::BinaryStringExecFunction<A, B, int32_t, FUNC>;
        break;
    case PhysicalTypeID::INT64:
        result = ScalarFunction::BinaryStringExecFunction<A, B, int64_t, FUNC>;
        break;
    case PhysicalTypeID::INT128:
        result = ScalarFunction::BinaryStringExecFunction<A, B, int128_t, FUNC>;
        break;
    default:
        KU_UNREACHABLE;
    }
}

template<typename FUNC, typename A>
static void getBinaryExecutionHelperA(const LogicalType& typeB, const LogicalType& typeR,
    scalar_func_exec_t& result) {
    // here to assist in getting scalar_func_exec_t for genericBinaryArithmeticFunc
    switch (typeB.getPhysicalType()) {
    case PhysicalTypeID::INT16:
        getBinaryExecutionHelperB<FUNC, A, int16_t>(typeR, result);
        break;
    case PhysicalTypeID::INT32:
        getBinaryExecutionHelperB<FUNC, A, int32_t>(typeR, result);
        break;
    case PhysicalTypeID::INT64:
        getBinaryExecutionHelperB<FUNC, A, int64_t>(typeR, result);
        break;
    case PhysicalTypeID::INT128:
        getBinaryExecutionHelperB<FUNC, A, int128_t>(typeR, result);
        break;
    default:
        KU_UNREACHABLE;
    }
}

template<typename FUNC>
static std::unique_ptr<FunctionBindData> genericBinaryArithmeticFunc(
    const binder::expression_vector& arguments, Function* func) {
    auto asScalar = ku_dynamic_cast<Function*, ScalarFunction*>(func);
    KU_ASSERT(asScalar != nullptr);
    auto argADataType = arguments[0]->getDataType();
    auto argBDataType = arguments[1]->getDataType();
    if (argADataType.getLogicalTypeID() != LogicalTypeID::DECIMAL) {
        argADataType = argBDataType;
    }
    if (argBDataType.getLogicalTypeID() != LogicalTypeID::DECIMAL) {
        argBDataType = argADataType;
    }
    auto precision1 = DecimalType::getPrecision(argADataType);
    auto precision2 = DecimalType::getPrecision(argBDataType);
    auto scale1 = DecimalType::getScale(argADataType);
    auto scale2 = DecimalType::getScale(argBDataType);
    auto params = FUNC::resultingParams(precision1, precision2, scale1, scale2);
    auto resultingType = LogicalType::DECIMAL(params.first, params.second);
    auto argumentAType = FUNC::matchToOutputLogicalType ? *resultingType : argADataType;
    auto argumentBType = FUNC::matchToOutputLogicalType ? *resultingType : argBDataType;
    if constexpr (FUNC::matchToOutputLogicalType) {
        switch (resultingType->getPhysicalType()) {
        case PhysicalTypeID::INT16:
            asScalar->execFunc =
                ScalarFunction::BinaryStringExecFunction<int16_t, int16_t, int16_t, FUNC>;
            break;
        case PhysicalTypeID::INT32:
            asScalar->execFunc =
                ScalarFunction::BinaryStringExecFunction<int32_t, int32_t, int32_t, FUNC>;
            break;
        case PhysicalTypeID::INT64:
            asScalar->execFunc =
                ScalarFunction::BinaryStringExecFunction<int64_t, int64_t, int64_t, FUNC>;
            break;
        case PhysicalTypeID::INT128:
            asScalar->execFunc =
                ScalarFunction::BinaryStringExecFunction<int128_t, int128_t, int128_t, FUNC>;
            break;
        default:
            KU_UNREACHABLE;
        }
    } else {
        switch (argumentAType.getPhysicalType()) {
        case PhysicalTypeID::INT16:
            getBinaryExecutionHelperA<FUNC, int16_t>(argumentBType, *resultingType,
                asScalar->execFunc);
            break;
        case PhysicalTypeID::INT32:
            getBinaryExecutionHelperA<FUNC, int32_t>(argumentBType, *resultingType,
                asScalar->execFunc);
            break;
        case PhysicalTypeID::INT64:
            getBinaryExecutionHelperA<FUNC, int64_t>(argumentBType, *resultingType,
                asScalar->execFunc);
            break;
        case PhysicalTypeID::INT128:
            getBinaryExecutionHelperA<FUNC, int128_t>(argumentBType, *resultingType,
                asScalar->execFunc);
            break;
        default:
            KU_UNREACHABLE;
        }
    }
    return std::make_unique<FunctionBindData>(
        std::vector<LogicalType>{argumentAType, argumentBType}, std::move(resultingType));
}

template<typename FUNC, typename ARG>
static void getUnaryExecutionHelper(const LogicalType& resultType, scalar_func_exec_t& result) {
    switch (resultType.getPhysicalType()) {
    case PhysicalTypeID::INT16:
        result = ScalarFunction::UnaryExecNestedTypeFunction<ARG, int16_t, FUNC>;
        break;
    case PhysicalTypeID::INT32:
        result = ScalarFunction::UnaryExecNestedTypeFunction<ARG, int32_t, FUNC>;
        break;
    case PhysicalTypeID::INT64:
        result = ScalarFunction::UnaryExecNestedTypeFunction<ARG, int64_t, FUNC>;
        break;
    case PhysicalTypeID::INT128:
        result = ScalarFunction::UnaryExecNestedTypeFunction<ARG, int128_t, FUNC>;
        break;
    default:
        KU_UNREACHABLE;
    }
}

template<typename FUNC>
static std::unique_ptr<FunctionBindData> genericUnaryArithmeticFunc(
    const binder::expression_vector& arguments, Function* func) {
    auto asScalar = ku_dynamic_cast<Function*, ScalarFunction*>(func);
    KU_ASSERT(asScalar != nullptr);
    auto argPrecision = DecimalType::getPrecision(arguments[0]->getDataType());
    auto argScale = DecimalType::getScale(arguments[0]->getDataType());
    auto params = FUNC::resultingParams(argPrecision, argScale);
    auto resultingType = LogicalType::DECIMAL(params.first, params.second);
    auto argumentType =
        FUNC::matchToOutputLogicalType ? *resultingType : arguments[0]->getDataType();
    if constexpr (FUNC::matchToOutputLogicalType) {
        switch (resultingType->getPhysicalType()) {
        case PhysicalTypeID::INT16:
            asScalar->execFunc =
                ScalarFunction::UnaryExecNestedTypeFunction<int16_t, int16_t, FUNC>;
            break;
        case PhysicalTypeID::INT32:
            asScalar->execFunc =
                ScalarFunction::UnaryExecNestedTypeFunction<int32_t, int32_t, FUNC>;
            break;
        case PhysicalTypeID::INT64:
            asScalar->execFunc =
                ScalarFunction::UnaryExecNestedTypeFunction<int64_t, int64_t, FUNC>;
            break;
        case PhysicalTypeID::INT128:
            asScalar->execFunc =
                ScalarFunction::UnaryExecNestedTypeFunction<int128_t, int128_t, FUNC>;
            break;
        default:
            KU_UNREACHABLE;
        }
    } else {
        switch (argumentType.getPhysicalType()) {
        case PhysicalTypeID::INT16:
            getUnaryExecutionHelper<FUNC, int16_t>(*resultingType, asScalar->execFunc);
            break;
        case PhysicalTypeID::INT32:
            getUnaryExecutionHelper<FUNC, int32_t>(*resultingType, asScalar->execFunc);
            break;
        case PhysicalTypeID::INT64:
            getUnaryExecutionHelper<FUNC, int64_t>(*resultingType, asScalar->execFunc);
            break;
        case PhysicalTypeID::INT128:
            getUnaryExecutionHelper<FUNC, int128_t>(*resultingType, asScalar->execFunc);
            break;
        default:
            KU_UNREACHABLE;
        }
    }
    return std::make_unique<FunctionBindData>(std::vector<LogicalType>{argumentType},
        std::move(resultingType));
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindAddFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericBinaryArithmeticFunc<DecimalAdd>(arguments, func);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindSubtractFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericBinaryArithmeticFunc<DecimalSubtract>(arguments, func);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindMultiplyFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericBinaryArithmeticFunc<DecimalMultiply>(arguments, func);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindDivideFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericBinaryArithmeticFunc<DecimalDivide>(arguments, func);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindModuloFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericBinaryArithmeticFunc<DecimalModulo>(arguments, func);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindNegateFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericUnaryArithmeticFunc<DecimalNegate>(arguments, func);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindAbsFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericUnaryArithmeticFunc<DecimalAbs>(arguments, func);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindFloorFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericUnaryArithmeticFunc<DecimalFloor>(arguments, func);
}

std::unique_ptr<FunctionBindData> DecimalFunction::bindCeilFunc(
    const binder::expression_vector& arguments, Function* func) {
    return genericUnaryArithmeticFunc<DecimalCeil>(arguments, func);
}

} // namespace function
} // namespace kuzu
