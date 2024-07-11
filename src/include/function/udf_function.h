#pragma once

#include "common/exception/binder.h"
#include "common/exception/catalog.h"
#include "common/types/blob.h"
#include "common/types/ku_string.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct UnaryUDFExecutor {
    template<class OPERAND_TYPE, class RESULT_TYPE>
    static inline void operation(OPERAND_TYPE& input, RESULT_TYPE& result, void* udfFunc) {
        typedef RESULT_TYPE (*unary_udf_func)(OPERAND_TYPE);
        auto unaryUDFFunc = (unary_udf_func)udfFunc;
        result = unaryUDFFunc(input);
    }
};

struct BinaryUDFExecutor {
    template<class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
    static inline void operation(LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result,
        void* udfFunc) {
        typedef RESULT_TYPE (*binary_udf_func)(LEFT_TYPE, RIGHT_TYPE);
        auto binaryUDFFunc = (binary_udf_func)udfFunc;
        result = binaryUDFFunc(left, right);
    }
};

struct TernaryUDFExecutor {
    template<class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE>
    static inline void operation(A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result,
        void* udfFunc) {
        typedef RESULT_TYPE (*ternary_udf_func)(A_TYPE, B_TYPE, C_TYPE);
        auto ternaryUDFFunc = (ternary_udf_func)udfFunc;
        result = ternaryUDFFunc(a, b, c);
    }
};

struct UDF {
    template<typename T>
    static bool templateValidateType(const common::LogicalTypeID& type) {
        switch (type) {
        case common::LogicalTypeID::BOOL:
            return std::is_same<T, bool>();
        case common::LogicalTypeID::INT16:
            return std::is_same<T, int16_t>();
        case common::LogicalTypeID::INT32:
            return std::is_same<T, int32_t>();
        case common::LogicalTypeID::INT64:
            return std::is_same<T, int64_t>();
        case common::LogicalTypeID::FLOAT:
            return std::is_same<T, float>();
        case common::LogicalTypeID::DOUBLE:
            return std::is_same<T, double>();
        case common::LogicalTypeID::DATE:
            return std::is_same<T, int32_t>();
        case common::LogicalTypeID::TIMESTAMP_NS:
        case common::LogicalTypeID::TIMESTAMP_MS:
        case common::LogicalTypeID::TIMESTAMP_SEC:
        case common::LogicalTypeID::TIMESTAMP_TZ:
        case common::LogicalTypeID::TIMESTAMP:
            return std::is_same<T, int64_t>();
        case common::LogicalTypeID::STRING:
            return std::is_same<T, common::ku_string_t>();
        case common::LogicalTypeID::BLOB:
            return std::is_same<T, common::blob_t>();
        default:
            KU_UNREACHABLE;
        }
    }

    template<typename T>
    static void validateType(const common::LogicalTypeID& type) {
        if (!templateValidateType<T>(type)) {
            throw common::CatalogException{
                "Incompatible udf parameter/return type and templated type."};
        }
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_func_exec_t createEmptyParameterExecFunc(RESULT_TYPE (*)(Args...),
        const std::vector<common::LogicalTypeID>&) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE>
    static function::scalar_func_exec_t createEmptyParameterExecFunc(RESULT_TYPE (*udfFunc)(),
        const std::vector<common::LogicalTypeID>&) {
        (void*)(udfFunc); // Disable compiler warnings.
        return [udfFunc](const std::vector<std::shared_ptr<common::ValueVector>>& params,
                   common::ValueVector& result, void* /*dataPtr*/ = nullptr) -> void {
            (void)params;
            KU_ASSERT(params.size() == 0);
            auto& resultSelVector = result.state->getSelVector();
            for (auto i = 0u; i < resultSelVector.getSelSize(); ++i) {
                auto resultPos = resultSelVector[i];
                result.copyFromValue(resultPos, common::Value(udfFunc()));
            }
        };
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_func_exec_t createUnaryExecFunc(RESULT_TYPE (* /*udfFunc*/)(Args...),
        const std::vector<common::LogicalTypeID>& /*parameterTypes*/) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE, typename OPERAND_TYPE>
    static function::scalar_func_exec_t createUnaryExecFunc(RESULT_TYPE (*udfFunc)(OPERAND_TYPE),
        const std::vector<common::LogicalTypeID>& parameterTypes) {
        if (parameterTypes.size() != 1) {
            throw common::CatalogException{
                "Expected exactly one parameter type for unary udf. Got: " +
                std::to_string(parameterTypes.size()) + "."};
        }
        validateType<OPERAND_TYPE>(parameterTypes[0]);
        function::scalar_func_exec_t execFunc =
            [udfFunc](const std::vector<std::shared_ptr<common::ValueVector>>& params,
                common::ValueVector& result, void* /*dataPtr*/ = nullptr) -> void {
            KU_ASSERT(params.size() == 1);
            UnaryFunctionExecutor::executeUDF<OPERAND_TYPE, RESULT_TYPE, UnaryUDFExecutor>(
                *params[0], result, (void*)udfFunc);
        };
        return execFunc;
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_func_exec_t createBinaryExecFunc(RESULT_TYPE (* /*udfFunc*/)(Args...),
        const std::vector<common::LogicalTypeID>& /*parameterTypes*/) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE, typename LEFT_TYPE, typename RIGHT_TYPE>
    static function::scalar_func_exec_t createBinaryExecFunc(
        RESULT_TYPE (*udfFunc)(LEFT_TYPE, RIGHT_TYPE),
        const std::vector<common::LogicalTypeID>& parameterTypes) {
        if (parameterTypes.size() != 2) {
            throw common::CatalogException{
                "Expected exactly two parameter types for binary udf. Got: " +
                std::to_string(parameterTypes.size()) + "."};
        }
        validateType<LEFT_TYPE>(parameterTypes[0]);
        validateType<RIGHT_TYPE>(parameterTypes[1]);
        function::scalar_func_exec_t execFunc =
            [udfFunc](const std::vector<std::shared_ptr<common::ValueVector>>& params,
                common::ValueVector& result, void* /*dataPtr*/ = nullptr) -> void {
            KU_ASSERT(params.size() == 2);
            BinaryFunctionExecutor::executeUDF<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE,
                BinaryUDFExecutor>(*params[0], *params[1], result, (void*)udfFunc);
        };
        return execFunc;
    }

    template<typename RESULT_TYPE, typename... Args>
    static function::scalar_func_exec_t createTernaryExecFunc(RESULT_TYPE (* /*udfFunc*/)(Args...),
        const std::vector<common::LogicalTypeID>& /*parameterTypes*/) {
        KU_UNREACHABLE;
    }

    template<typename RESULT_TYPE, typename A_TYPE, typename B_TYPE, typename C_TYPE>
    static function::scalar_func_exec_t createTernaryExecFunc(
        RESULT_TYPE (*udfFunc)(A_TYPE, B_TYPE, C_TYPE),
        std::vector<common::LogicalTypeID> parameterTypes) {
        if (parameterTypes.size() != 3) {
            throw common::CatalogException{
                "Expected exactly three parameter types for ternary udf. Got: " +
                std::to_string(parameterTypes.size()) + "."};
        }
        validateType<A_TYPE>(parameterTypes[0]);
        validateType<B_TYPE>(parameterTypes[1]);
        validateType<C_TYPE>(parameterTypes[2]);
        function::scalar_func_exec_t execFunc =
            [udfFunc](const std::vector<std::shared_ptr<common::ValueVector>>& params,
                common::ValueVector& result, void* /*dataPtr*/ = nullptr) -> void {
            KU_ASSERT(params.size() == 3);
            TernaryFunctionExecutor::executeUDF<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE,
                TernaryUDFExecutor>(*params[0], *params[1], *params[2], result, (void*)udfFunc);
        };
        return execFunc;
    }

    template<typename TR, typename... Args>
    static scalar_func_exec_t getScalarExecFunc(TR (*udfFunc)(Args...),
        std::vector<common::LogicalTypeID> parameterTypes) {
        constexpr auto numArgs = sizeof...(Args);
        switch (numArgs) {
        case 0:
            return createEmptyParameterExecFunc<TR, Args...>(udfFunc, std::move(parameterTypes));
        case 1:
            return createUnaryExecFunc<TR, Args...>(udfFunc, std::move(parameterTypes));
        case 2:
            return createBinaryExecFunc<TR, Args...>(udfFunc, std::move(parameterTypes));
        case 3:
            return createTernaryExecFunc<TR, Args...>(udfFunc, std::move(parameterTypes));
        default:
            throw common::BinderException("UDF function only supported until ternary!");
        }
    }

    template<typename T>
    static common::LogicalTypeID getParameterType() {
        if (std::is_same<T, bool>()) {
            return common::LogicalTypeID::BOOL;
        } else if (std::is_same<T, int16_t>()) {
            return common::LogicalTypeID::INT16;
        } else if (std::is_same<T, int32_t>()) {
            return common::LogicalTypeID::INT32;
        } else if (std::is_same<T, int64_t>()) {
            return common::LogicalTypeID::INT64;
        } else if (std::is_same<T, float>()) {
            return common::LogicalTypeID::FLOAT;
        } else if (std::is_same<T, double>()) {
            return common::LogicalTypeID::DOUBLE;
        } else if (std::is_same<T, common::ku_string_t>()) {
            return common::LogicalTypeID::STRING;
        } else {
            KU_UNREACHABLE;
        }
    }

    template<typename TA>
    static void getParameterTypesRecursive(std::vector<common::LogicalTypeID>& arguments) {
        arguments.push_back(getParameterType<TA>());
    }

    template<typename TA, typename TB, typename... Args>
    static void getParameterTypesRecursive(std::vector<common::LogicalTypeID>& arguments) {
        arguments.push_back(getParameterType<TA>());
        getParameterTypesRecursive<TB, Args...>(arguments);
    }

    template<typename... Args>
    static std::vector<common::LogicalTypeID> getParameterTypes() {
        std::vector<common::LogicalTypeID> parameterTypes;
        if constexpr (sizeof...(Args) > 0) {
            getParameterTypesRecursive<Args...>(parameterTypes);
        }
        return parameterTypes;
    }

    template<typename TR, typename... Args>
    static function_set getFunction(std::string name, TR (*udfFunc)(Args...),
        std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType) {
        function_set definitions;
        if (returnType == common::LogicalTypeID::STRING) {
            KU_UNREACHABLE;
        }
        validateType<TR>(returnType);
        scalar_func_exec_t scalarExecFunc = getScalarExecFunc<TR, Args...>(udfFunc, parameterTypes);
        definitions.push_back(std::make_unique<function::ScalarFunction>(std::move(name),
            std::move(parameterTypes), returnType, std::move(scalarExecFunc)));
        return definitions;
    }

    template<typename TR, typename... Args>
    static function_set getFunction(std::string name, TR (*udfFunc)(Args...)) {
        return getFunction<TR, Args...>(std::move(name), udfFunc, getParameterTypes<Args...>(),
            getParameterType<TR>());
    }

    template<typename TR, typename... Args>
    static function_set getVectorizedFunction(std::string name, scalar_func_exec_t execFunc) {
        function_set definitions;
        definitions.push_back(std::make_unique<function::ScalarFunction>(std::move(name),
            getParameterTypes<Args...>(), getParameterType<TR>(), std::move(execFunc)));
        return definitions;
    }

    static function_set getVectorizedFunction(std::string name, scalar_func_exec_t execFunc,
        std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType) {
        function_set definitions;
        definitions.push_back(std::make_unique<function::ScalarFunction>(std::move(name),
            std::move(parameterTypes), returnType, std::move(execFunc)));
        return definitions;
    }
};

} // namespace function
} // namespace kuzu
