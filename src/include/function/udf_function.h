#pragma once

#include "function/vector_functions.h"

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
    static inline void operation(
        LEFT_TYPE& left, RIGHT_TYPE& right, RESULT_TYPE& result, void* udfFunc) {
        typedef RESULT_TYPE (*binary_udf_func)(LEFT_TYPE, RIGHT_TYPE);
        auto binaryUDFFunc = (binary_udf_func)udfFunc;
        result = binaryUDFFunc(left, right);
    }
};

struct TernaryUDFExecutor {
    template<class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE>
    static inline void operation(
        A_TYPE& a, B_TYPE& b, C_TYPE& c, RESULT_TYPE& result, void* udfFunc) {
        typedef RESULT_TYPE (*ternary_udf_func)(A_TYPE, B_TYPE, C_TYPE);
        auto ternaryUDFFunc = (ternary_udf_func)udfFunc;
        result = ternaryUDFFunc(a, b, c);
    }
};

template<typename RESULT_TYPE, typename... Args>
static inline function::scalar_exec_func createUnaryExecFunc(RESULT_TYPE (*udfFunc)(Args...)) {
    throw common::NotImplementedException{"function::createUnaryExecFunc()"};
}

template<typename RESULT_TYPE, typename OPERAND_TYPE>
static inline function::scalar_exec_func createUnaryExecFunc(RESULT_TYPE (*udfFunc)(OPERAND_TYPE)) {
    function::scalar_exec_func execFunc =
        [=](const std::vector<std::shared_ptr<common::ValueVector>>& params,
            common::ValueVector& result) -> void {
        assert(params.size() == 1);
        UnaryFunctionExecutor::executeUDF<OPERAND_TYPE, RESULT_TYPE, UnaryUDFExecutor>(
            *params[0], result, (void*)udfFunc);
    };
    return execFunc;
}

template<typename RESULT_TYPE, typename... Args>
static inline function::scalar_exec_func createBinaryExecFunc(RESULT_TYPE (*udfFunc)(Args...)) {
    throw common::NotImplementedException{"function::createBinaryExecFunc()"};
}

template<typename RESULT_TYPE, typename LEFT_TYPE, typename RIGHT_TYPE>
static inline function::scalar_exec_func createBinaryExecFunc(
    RESULT_TYPE (*udfFunc)(LEFT_TYPE, RIGHT_TYPE)) {
    function::scalar_exec_func execFunc =
        [=](const std::vector<std::shared_ptr<common::ValueVector>>& params,
            common::ValueVector& result) -> void {
        assert(params.size() == 2);
        BinaryFunctionExecutor::executeUDF<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, BinaryUDFExecutor>(
            *params[0], *params[1], result, (void*)udfFunc);
    };
    return execFunc;
}

template<typename RESULT_TYPE, typename... Args>
static inline function::scalar_exec_func createTernaryExecFunc(RESULT_TYPE (*udfFunc)(Args...)) {
    throw common::NotImplementedException{"function::createTernaryExecFunc()"};
}

template<typename RESULT_TYPE, typename A_TYPE, typename B_TYPE, typename C_TYPE>
static inline function::scalar_exec_func createTernaryExecFunc(
    RESULT_TYPE (*udfFunc)(A_TYPE, B_TYPE, C_TYPE)) {
    function::scalar_exec_func execFunc =
        [=](const std::vector<std::shared_ptr<common::ValueVector>>& params,
            common::ValueVector& result) -> void {
        assert(params.size() == 3);
        TernaryFunctionExecutor::executeUDF<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE,
            TernaryUDFExecutor>(*params[0], *params[1], *params[2], result, (void*)udfFunc);
    };
    return execFunc;
}

template<typename TR, typename... Args>
inline static scalar_exec_func getScalarExecFunc(TR (*udfFunc)(Args...)) {
    constexpr auto numArgs = sizeof...(Args);
    switch (numArgs) {
    case 1:
        return createUnaryExecFunc<TR, Args...>(udfFunc);
    case 2:
        return createBinaryExecFunc<TR, Args...>(udfFunc);
    case 3:
        return createTernaryExecFunc<TR, Args...>(udfFunc);
    default:
        throw common::BinderException("UDF function only supported until ternary!");
    }
}

template<typename T>
inline static common::LogicalTypeID getParameterType() {
    if (std::is_same<T, bool>()) {
        return common::LogicalTypeID::BOOL;
    } else if (std::is_same<T, int16_t>()) {
        return common::LogicalTypeID::INT16;
    } else if (std::is_same<T, int32_t>()) {
        return common::LogicalTypeID::INT32;
    } else if (std::is_same<T, int64_t>()) {
        return common::LogicalTypeID::INT64;
    } else if (std::is_same<T, float_t>()) {
        return common::LogicalTypeID::FLOAT;
    } else if (std::is_same<T, double_t>()) {
        return common::LogicalTypeID::DOUBLE;
    } else if (std::is_same<T, common::ku_string_t>()) {
        return common::LogicalTypeID::STRING;
    } else {
        throw common::NotImplementedException{"function::getParameterType"};
    }
}

template<typename TA>
inline static void getParameterTypesRecursive(std::vector<common::LogicalTypeID>& arguments) {
    arguments.push_back(getParameterType<TA>());
}

template<typename TA, typename TB, typename... Args>
inline static void getParameterTypesRecursive(std::vector<common::LogicalTypeID>& arguments) {
    arguments.push_back(getParameterType<TA>());
    getParameterTypesRecursive<TB, Args...>(arguments);
}

template<typename TR, typename... Args>
inline static std::unique_ptr<VectorFunctionDefinition> getFunctionDefinition(
    const std::string& name, TR (*udfFunc)(Args...),
    std::vector<common::LogicalTypeID> parameterTypes, common::LogicalTypeID returnType) {
    function::scalar_exec_func scalarExecFunc = function::getScalarExecFunc<TR, Args...>(udfFunc);
    return std::make_unique<function::VectorFunctionDefinition>(
        name, std::move(parameterTypes), returnType, std::move(scalarExecFunc));
}

template<typename TR, typename... Args>
inline static std::unique_ptr<VectorFunctionDefinition> getFunctionDefinition(
    const std::string& name, TR (*udfFunc)(Args...)) {
    std::vector<common::LogicalTypeID> parameterTypes;
    getParameterTypesRecursive<Args...>(parameterTypes);
    common::LogicalTypeID returnType = getParameterType<TR>();
    if (returnType == common::LogicalTypeID::STRING) {
        throw common::NotImplementedException{"function::getFunctionDefinition"};
    }
    return getFunctionDefinition<TR, Args...>(name, udfFunc, std::move(parameterTypes), returnType);
}

} // namespace function
} // namespace kuzu
