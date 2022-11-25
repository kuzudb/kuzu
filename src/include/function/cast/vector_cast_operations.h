#pragma once

#include "cast_operations.h"
#include "function/vector_operations.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

/**
 *  In the system we define explicit cast and implicit cast.
 *  Explicit casts are performed from user function calls, e.g. date(), string().
 *  Implicit casts are added internally.
 */
class VectorCastOperations : public VectorOperations {
public:
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
} // namespace kuzu
