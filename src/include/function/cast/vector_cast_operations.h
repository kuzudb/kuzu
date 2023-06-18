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
    static bool hasImplicitCast(
        const common::LogicalType& srcType, const common::LogicalType& dstType);
    static std::string bindImplicitCastFuncName(const common::LogicalType& dstType);
    static void bindImplicitCastFunc(common::LogicalTypeID sourceTypeID,
        common::LogicalTypeID targetTypeID, scalar_exec_func& func);

protected:
    template<typename SOURCE_TYPE, typename TARGET_TYPE, typename FUNC>
    inline static std::unique_ptr<VectorOperationDefinition> bindVectorOperation(
        const std::string& funcName, common::LogicalTypeID sourceTypeID,
        common::LogicalTypeID targetTypeID) {
        return std::make_unique<VectorOperationDefinition>(funcName,
            std::vector<common::LogicalTypeID>{sourceTypeID}, targetTypeID,
            UnaryExecFunction<SOURCE_TYPE, TARGET_TYPE, FUNC>);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryCastExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryOperationExecutor::executeCast<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0], result);
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
                "Unimplemented casting operation from " +
                common::LogicalTypeUtils::dataTypeToString(srcTypeID) + " to numeric.");
        }
    }
};

struct CastToDateVectorOperation : public VectorCastOperations {
    static vector_operation_definitions getDefinitions();
};

struct CastToTimestampVectorOperation : public VectorCastOperations {
    static vector_operation_definitions getDefinitions();
};

struct CastToIntervalVectorOperation : public VectorCastOperations {
    static vector_operation_definitions getDefinitions();
};

struct CastToStringVectorOperation : public VectorCastOperations {
    static vector_operation_definitions getDefinitions();
};

struct CastToBlobVectorOperation : public VectorCastOperations {
    static vector_operation_definitions getDefinitions();
};

struct CastToDoubleVectorOperation : public VectorCastOperations {
    static vector_operation_definitions getDefinitions();
};

struct CastToFloatVectorOperation : public VectorCastOperations {
    static vector_operation_definitions getDefinitions();
};

struct CastToInt64VectorOperation : public VectorCastOperations {
    static vector_operation_definitions getDefinitions();
};

struct CastToInt32VectorOperation : public VectorCastOperations {
    static vector_operation_definitions getDefinitions();
};

struct CastToInt16VectorOperation : public VectorCastOperations {
    static vector_operation_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
