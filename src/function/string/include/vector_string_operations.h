#pragma once

#include "src/function/include/vector_operations.h"

namespace graphflow {
namespace function {

struct VectorStringOperations : public VectorOperations {

public:
    static scalar_exec_func bindExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static scalar_select_func bindSelectFunction(
        ExpressionType expressionType, const expression_vector& children);

private:
    static scalar_exec_func bindBinaryExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static scalar_select_func bindBinarySelectFunction(
        ExpressionType expressionType, const expression_vector& children);
};

} // namespace function
} // namespace graphflow
