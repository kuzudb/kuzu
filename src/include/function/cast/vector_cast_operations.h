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
    static bool hasImplicitCast(const common::DataType& srcType, const common::DataType& dstType);
    static std::string bindImplicitCastFuncName(const common::DataType& dstType);
    static scalar_exec_func bindImplicitCastFunc(
        common::DataTypeID sourceTypeID, common::DataTypeID targetTypeID);

    template<typename SOURCE_TYPE, typename TARGET_TYPE, typename FUNC>
    inline static std::unique_ptr<VectorOperationDefinition> bindVectorOperation(
        const std::string& funcName, common::DataTypeID sourceTypeID,
        common::DataTypeID targetTypeID) {
        return std::make_unique<VectorOperationDefinition>(funcName,
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

private:
    template<typename DST_TYPE, typename OP>
    static scalar_exec_func bindImplicitNumericalCastFunc(common::DataTypeID srcTypeID) {
        switch (srcTypeID) {
        case common::INT16:
            return VectorOperations::UnaryExecFunction<int16_t, DST_TYPE, OP>;
        case common::INT32:
            return VectorOperations::UnaryExecFunction<int32_t, DST_TYPE, OP>;
        case common::INT64:
            return VectorOperations::UnaryExecFunction<int64_t, DST_TYPE, OP>;
        case common::FLOAT:
            return VectorOperations::UnaryExecFunction<float_t, DST_TYPE, OP>;
        case common::DOUBLE:
            return VectorOperations::UnaryExecFunction<double_t, DST_TYPE, OP>;
        default:
            throw common::NotImplementedException("Unimplemented casting operation from " +
                                                  common::Types::dataTypeToString(srcTypeID) +
                                                  " to numeric.");
        }
    }

    template<typename DST_TYPE, typename OP>
    static scalar_exec_func bindImplicitStringCastFunc(common::DataTypeID srcTypeID) {
        switch (srcTypeID) {
        case common::INT64:
            return UnaryCastExecFunction<int64_t, DST_TYPE, OP>;
        case common::DOUBLE:
            return UnaryCastExecFunction<double_t, DST_TYPE, OP>;
        case common::DATE:
            return UnaryCastExecFunction<common::date_t, DST_TYPE, OP>;
        case common::TIMESTAMP:
            return UnaryCastExecFunction<common::timestamp_t, DST_TYPE, OP>;
        case common::INTERVAL:
            return UnaryCastExecFunction<common::interval_t, DST_TYPE, OP>;
        case common::VAR_LIST:
            return UnaryCastExecFunction<common::ku_list_t, DST_TYPE, OP>;
        default:
            throw common::NotImplementedException("Unimplemented casting operation from " +
                                                  common::Types::dataTypeToString(srcTypeID) +
                                                  " to string.");
        }
    }
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

struct CastToInt16VectorOperation : public VectorCastOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

} // namespace function
} // namespace kuzu
