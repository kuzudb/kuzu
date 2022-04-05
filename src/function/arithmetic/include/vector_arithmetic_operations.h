#pragma once

#include "src/function/include/vector_operations.h"

namespace graphflow {
namespace function {

class VectorArithmeticOperations : public VectorOperations {

public:
    static pair<scalar_exec_func, DataType> bindExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindAbsExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindFloorExecFunction(
        const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindCeilExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindSinExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindCosExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindTanExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindCotExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindAsinExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindAcosExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindAtanExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindEvenExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindFactorialExecFunction(
        const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindSignExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindSqrtExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindCbrtExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindGammaExecFunction(
        const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindLgammaExecFunction(
        const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindLnExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindLogExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindLog2ExecFunction(const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindDegreesExecFunction(
        const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindRadiansExecFunction(
        const expression_vector& children);

private:
    static pair<scalar_exec_func, DataType> bindBinaryExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindStringArithmeticExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindDateArithmeticExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindTimestampArithmeticExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindIntervalArithmeticExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static pair<scalar_exec_func, DataType> bindNumericalArithmeticExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE>
    static scalar_exec_func bindNumericalArithmeticExecFunction(ExpressionType expressionType);

    static pair<scalar_exec_func, DataType> bindUnaryExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    template<typename FUNC>
    static pair<scalar_exec_func, DataType> bindUnaryExecFunction(const DataType& operandType);
};

} // namespace function
} // namespace graphflow
