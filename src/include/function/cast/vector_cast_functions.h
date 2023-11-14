#pragma once

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

    template<typename EXECUTOR = UnaryFunctionExecutor>
    static std::unique_ptr<ScalarFunction> bindCastFunction(const std::string& functionName,
        common::LogicalTypeID sourceTypeID, common::LogicalTypeID targetTypeID);
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

struct CastAnyFunction {
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
