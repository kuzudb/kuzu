#pragma once

#include "src/function/cast/operations/include/cast_operations.h"
#include "src/function/include/vector_operations.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {

/**
 *  In the system we define explicit cast and implicit cast.
 *  Explicit casts are performed from user function calls, e.g. date(), string().
 *  Implicit casts are added internally. The rules are as follows:
 *  *  Implicit cast from unstructured to structured if function is not overload, e.g. AND,
 *     CONTAINS.
 *  *  Implicit cast from structured to unstructured if function is overload and at least one
 *     parameter is unstructured.
 */
class VectorCastOperations : public VectorOperations {

public:
    static scalar_exec_func bindImplicitCastToBool(const expression_vector& children);

    static scalar_exec_func bindImplicitCastToInt64(const expression_vector& children);

    static scalar_exec_func bindImplicitCastToString(const expression_vector& children);

    static scalar_exec_func bindImplicitCastToTimestamp(const expression_vector& children);

    static scalar_exec_func bindImplicitCastToUnstructured(const expression_vector& children);

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryCastExecFunction(
        const vector<shared_ptr<ValueVector>>& params, ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::executeCast<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0], result);
    }
};

struct CastToDateVectorOperation : public VectorCastOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CastToTimestampVectorOperation : public VectorCastOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CastToIntervalVectorOperation : public VectorCastOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CastToStringVectorOperation : public VectorCastOperations {
    static vector<unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace graphflow
