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
    static bool hasImplicitCast(const common::LogicalType& srcType,
        const common::LogicalType& dstType);

    template<typename EXECUTOR = UnaryFunctionExecutor>
    static std::unique_ptr<ScalarFunction> bindCastFunction(const std::string& functionName,
        common::LogicalTypeID sourceTypeID, common::LogicalTypeID targetTypeID);
};

struct CastToDateFunction {
    static constexpr const char* name = "TO_DATE";

    static constexpr const char* alias = "DATE";

    static function_set getFunctionSet();
};

struct CastToTimestampFunction {
    static constexpr const char* name = "TIMESTAMP";

    static function_set getFunctionSet();
};

struct CastToIntervalFunction {
    static constexpr const char* name = "TO_INTERVAL";

    static constexpr const char* alias = "INTERVAL";

    static function_set getFunctionSet();
};

struct CastToStringFunction {
    static constexpr const char* name = "TO_STRING";

    static constexpr const char* alias = "STRING";

    static function_set getFunctionSet();
};

struct CastToBlobFunction {
    static constexpr const char* name = "TO_BLOB";

    static constexpr const char* alias = "BLOB";

    static function_set getFunctionSet();
};

struct CastToUUIDFunction {
    static constexpr const char* name = "TO_UUID";

    static constexpr const char* alias = "UUID";

    static function_set getFunctionSet();
};

struct CastToBoolFunction {
    static constexpr const char* name = "TO_BOOL";

    static function_set getFunctionSet();
};

struct CastToDoubleFunction {
    static constexpr const char* name = "TO_DOUBLE";

    static function_set getFunctionSet();
};

struct CastToFloatFunction {
    static constexpr const char* name = "TO_FLOAT";

    static function_set getFunctionSet();
};

struct CastToSerialFunction {
    static constexpr const char* name = "TO_SERIAL";

    static function_set getFunctionSet();
};

struct CastToInt128Function {
    static constexpr const char* name = "TO_INT128";

    static function_set getFunctionSet();
};

struct CastToInt64Function {
    static constexpr const char* name = "TO_INT64";

    static function_set getFunctionSet();
};

struct CastToInt32Function {
    static constexpr const char* name = "TO_INT32";

    static function_set getFunctionSet();
};

struct CastToInt16Function {
    static constexpr const char* name = "TO_INT16";

    static function_set getFunctionSet();
};

struct CastToInt8Function {
    static constexpr const char* name = "TO_INT8";

    static function_set getFunctionSet();
};

struct CastToUInt64Function {
    static constexpr const char* name = "TO_UINT64";

    static function_set getFunctionSet();
};

struct CastToUInt32Function {
    static constexpr const char* name = "TO_UINT32";

    static function_set getFunctionSet();
};

struct CastToUInt16Function {
    static constexpr const char* name = "TO_UINT16";

    static function_set getFunctionSet();
};

struct CastToUInt8Function {
    static constexpr const char* name = "TO_UINT8";

    static function_set getFunctionSet();
};

struct CastAnyFunction {
    static constexpr const char* name = "CAST";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
