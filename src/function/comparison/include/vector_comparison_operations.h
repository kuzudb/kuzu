#pragma once

#include "src/binder/expression/include/expression.h"
#include "src/function/comparison/operations/include/comparison_operations.h"
#include "src/function/include/vector_operations.h"

namespace graphflow {
namespace function {

class VectorComparisonOperations : public VectorOperations {

public:
    static scalar_exec_func bindExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static scalar_select_func bindSelectFunction(
        ExpressionType expressionType, const expression_vector& children);

private:
    static scalar_exec_func bindBinaryExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    template<typename LEFT_TYPE, typename RIGHT_TYPE>
    static scalar_exec_func bindBinaryExecFunction(ExpressionType expressionType);

    static scalar_select_func bindBinarySelectFunction(
        ExpressionType expressionType, const expression_vector& children);

    template<typename LEFT_TYPE, typename RIGHT_TYPE>
    static scalar_select_func bindBinarySelectFunction(ExpressionType expressionType);

    static void validate(ExpressionType expressionType, DataType leftType, DataType rightType);
};

} // namespace function
} // namespace graphflow
