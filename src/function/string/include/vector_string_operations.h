#pragma once

#include "src/function/include/vector_operations.h"
#include "src/function/string/operations/include/length_operation.h"

namespace graphflow {
namespace function {

struct VectorStringOperations : public VectorOperations {

public:
    static scalar_exec_func bindExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static scalar_select_func bindSelectFunction(
        ExpressionType expressionType, const expression_vector& children);

    template<typename FUNC>
    static pair<scalar_exec_func, DataType> bindUnaryStringFunction(
        const expression_vector& children, string funcName) {
        VectorOperations::validateNumParameters(funcName, children.size(), 1);
        VectorOperations::validateParameterType(funcName, *children[0], {STRING, UNSTRUCTURED});
        switch (children[0]->dataType.typeID) {
        case STRING:
            if constexpr (is_same<FUNC, operation::Length>::value) {
                return make_pair(VectorOperations::UnaryExecFunction<gf_string_t, int64_t, FUNC>,
                    DataType(INT64));
            } else {
                return make_pair(
                    VectorOperations::UnaryExecFunction<gf_string_t, gf_string_t, FUNC>,
                    DataType(STRING));
            }
        case UNSTRUCTURED:
            return make_pair(
                VectorOperations::UnaryExecFunction<Value, Value, FUNC>, DataType(UNSTRUCTURED));
        default:
            assert(false);
        }
    }

private:
    static scalar_exec_func bindBinaryExecFunction(
        ExpressionType expressionType, const expression_vector& children);

    static scalar_select_func bindBinarySelectFunction(
        ExpressionType expressionType, const expression_vector& children);
};

} // namespace function
} // namespace graphflow
