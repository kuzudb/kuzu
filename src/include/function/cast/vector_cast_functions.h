#pragma once

#include "cast_functions.h"
#include "function/vector_functions.h"

namespace kuzu {
namespace function {

/**
 *  In the system we define explicit cast and implicit cast.
 *  Explicit casts are performed from user function calls, e.g. date(), string().
 *  Implicit casts are added internally.
 */
class VectorCastFunction : public VectorFunction {
public:
    // This function is only used by expression binder when implicit cast is needed.
    // The expression binder should consider reusing the existing matchFunction() API.
    static bool hasImplicitCast(
        const common::LogicalType& srcType, const common::LogicalType& dstType);
    static std::string bindImplicitCastFuncName(const common::LogicalType& dstType);
    static void bindImplicitCastFunc(common::LogicalTypeID sourceTypeID,
        common::LogicalTypeID targetTypeID, scalar_exec_func& func);

protected:
    template<typename SOURCE_TYPE, typename TARGET_TYPE, typename FUNC>
    inline static std::unique_ptr<VectorFunctionDefinition> bindVectorFunction(
        const std::string& funcName, common::LogicalTypeID sourceTypeID,
        common::LogicalTypeID targetTypeID) {
        return std::make_unique<VectorFunctionDefinition>(funcName,
            std::vector<common::LogicalTypeID>{sourceTypeID}, targetTypeID,
            UnaryExecFunction<SOURCE_TYPE, TARGET_TYPE, FUNC>);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryCastExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryFunctionExecutor::executeCast<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0], result);
    }

    template<typename DST_TYPE, typename OP>
    static void bindImplicitNumericalCastFunc(
        common::LogicalTypeID srcTypeID, scalar_exec_func& func) {
        switch (srcTypeID) {
        case common::LogicalTypeID::INT16: {
            func = UnaryExecFunction<int16_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::INT32: {
            func = UnaryExecFunction<int32_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::SERIAL:
        case common::LogicalTypeID::INT64: {
            func = UnaryExecFunction<int64_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::FLOAT: {
            func = UnaryExecFunction<float_t, DST_TYPE, OP>;
            return;
        }
        case common::LogicalTypeID::DOUBLE: {
            func = UnaryExecFunction<double_t, DST_TYPE, OP>;
            return;
        }
        default:
            throw common::NotImplementedException(
                "Unimplemented casting Function from " +
                common::LogicalTypeUtils::dataTypeToString(srcTypeID) + " to numeric.");
        }
    }
};

struct CastToDateVectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

struct CastToTimestampVectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

struct CastToIntervalVectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

struct CastToStringVectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

struct CastToBlobVectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

struct CastToDoubleVectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

struct CastToFloatVectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

struct CastToSerialVectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

struct CastToInt64VectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

struct CastToInt32VectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

struct CastToInt16VectorFunction : public VectorCastFunction {
    static vector_function_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
