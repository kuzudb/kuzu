#pragma once

#include "function/cast/functions/cast_functions.h"
#include "function/cast/functions/cast_string_to_functions.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

/**
 *  In the system we define explicit cast and implicit cast.
 *  Explicit casts are performed from user function calls, e.g. date(), string().
 *  Implicit casts are added internally.
 */
struct CastFunction {
    // This function is only used by expression binder when implicit cast is needed.
    // The expression binder should consider reusing the existing matchFunction() API.
    static bool hasImplicitCast(
        const common::LogicalType& srcType, const common::LogicalType& dstType);
    static std::string bindImplicitCastFuncName(const common::LogicalType& dstType);
    static void bindImplicitCastFunc(common::LogicalTypeID sourceTypeID,
        common::LogicalTypeID targetTypeID, scalar_exec_func& func);

    template<typename TARGET_TYPE, typename FUNC>
    inline static std::unique_ptr<ScalarFunction> bindNumericCastFunction(
        const std::string& funcName, common::LogicalTypeID sourceTypeID,
        common::LogicalTypeID targetTypeID) {
        scalar_exec_func func;
        bindImplicitNumericalCastFunc<TARGET_TYPE, FUNC>(sourceTypeID, func);
        return std::make_unique<ScalarFunction>(
            funcName, std::vector<common::LogicalTypeID>{sourceTypeID}, targetTypeID, func);
    }

    template<typename DST_TYPE, typename OP>
    static void bindImplicitNumericalCastFunc(
        common::LogicalTypeID srcTypeID, scalar_exec_func& func) {
        switch (srcTypeID) {
        case common::LogicalTypeID::INT8: {
            func = ScalarFunction::UnaryExecFunction<int8_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::INT16: {
            func = ScalarFunction::UnaryExecFunction<int16_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::INT32: {
            func = ScalarFunction::UnaryExecFunction<int32_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            func = ScalarFunction::UnaryExecFunction<int64_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::UINT8: {
            func = ScalarFunction::UnaryExecFunction<uint8_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::UINT16: {
            func = ScalarFunction::UnaryExecFunction<uint16_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::UINT32: {
            func = ScalarFunction::UnaryExecFunction<uint32_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::UINT64: {
            func = ScalarFunction::UnaryExecFunction<uint64_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::INT128: {
            func = ScalarFunction::UnaryExecFunction<common::int128_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::FLOAT: {
            func = ScalarFunction::UnaryExecFunction<float_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::DOUBLE: {
            func = ScalarFunction::UnaryExecFunction<double_t, DST_TYPE, OP>;
            return;
        }
        default:
            throw common::NotImplementedException(
                "Unimplemented casting Function from " +
                common::LogicalTypeUtils::dataTypeToString(srcTypeID) + " to numeric.");
        }
    }

    template<typename TARGET_TYPE>
    inline static std::unique_ptr<ScalarFunction> bindCastStringToFunction(
        const std::string& funcName, common::LogicalTypeID targetTypeID) {
        return std::make_unique<ScalarFunction>(funcName,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING}, targetTypeID,
            ScalarFunction::UnaryStringExecFunction<common::ku_string_t, TARGET_TYPE,
                CastStringToTypes>);
    }
};

struct CastToDateFunction {
    static function_set getFunctionSet();
};

struct CastToTimestampFunction {
    static function_set getFunctionSet();
};

struct CastToIntervalFunction {
    static function_set getFunctionSet();
};

struct CastToStringFunction {
    static void getUnaryCastToStringExecFunction(
        common::LogicalTypeID typeID, scalar_exec_func& func);
    static function_set getFunctionSet();
};

struct CastToBlobFunction {
    static function_set getFunctionSet();
};

struct CastToBoolFunction {
    static function_set getFunctionSet();
};

struct CastToDoubleFunction {
    static function_set getFunctionSet();
};

struct CastToFloatFunction {
    static function_set getFunctionSet();
};

struct CastToSerialFunction {
    static function_set getFunctionSet();
};

struct CastToInt128Function {
    static function_set getFunctionSet();
};

struct CastToInt64Function {
    static function_set getFunctionSet();
};

struct CastToInt32Function {
    static function_set getFunctionSet();
};

struct CastToInt16Function {
    static function_set getFunctionSet();
};

struct CastToInt8Function {
    static function_set getFunctionSet();
};

struct CastToUInt64Function {
    static function_set getFunctionSet();
};

struct CastToUInt32Function {
    static function_set getFunctionSet();
};

struct CastToUInt16Function {
    static function_set getFunctionSet();
};

struct CastToUInt8Function {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
