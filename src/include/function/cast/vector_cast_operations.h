#pragma once

#include "cast_operations.h"
#include "function/vector_operations.h"

namespace kuzu {
namespace function {

/**
 *  In the system we define explicit cast and implicit cast.
 *  Explicit casts are performed from user function calls, e.g. date(), string().
 *  Implicit casts are added internally.
 */
class VectorCastOperations : public VectorOperations {
public:
    // This function is only used by expression binder when implicit cast is needed.
    // The expression binder should consider reusing the existing matchFunction() API.
    static scalar_exec_func bindImplicitCastFunc(
        common::DataTypeID sourceTypeID, common::DataTypeID targetTypeID);

    template<typename SOURCE_TYPE, typename TARGET_TYPE, typename FUNC>
    inline static std::unique_ptr<VectorOperationDefinition> bindVectorOperation(
        const std::string& funcName, common::DataTypeID sourceTypeID,
        common::DataTypeID targetTypeID) {
        return make_unique<VectorOperationDefinition>(funcName,
            std::vector<common::DataTypeID>{sourceTypeID}, targetTypeID,
            VectorOperations::UnaryExecFunction<SOURCE_TYPE, TARGET_TYPE, FUNC>);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryCastExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::executeCast<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0], result);
    }

    static std::string bindCastFunctionName(common::DataTypeID targetTypeID);

private:
    static scalar_exec_func bindImplicitCastInt16Func(common::DataTypeID targetTypeID);
    static scalar_exec_func bindImplicitCastInt32Func(common::DataTypeID targetTypeID);
    static scalar_exec_func bindImplicitCastInt64Func(common::DataTypeID targetTypeID);
    static scalar_exec_func bindImplicitCastFloatFunc(common::DataTypeID targetTypeID);
};

struct CastToDateVectorOperation : public VectorCastOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CastToTimestampVectorOperation : public VectorCastOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CastToIntervalVectorOperation : public VectorCastOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CastToStringVectorOperation : public VectorCastOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CastToDoubleVectorOperation : public VectorCastOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CastToFloatVectorOperation : public VectorCastOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CastToInt64VectorOperation : public VectorCastOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct CastToInt32VectorOperation : public VectorCastOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace kuzu
