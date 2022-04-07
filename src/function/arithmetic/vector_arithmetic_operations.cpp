#include "include/vector_arithmetic_operations.h"

#include "operations/include/arithmetic_operations.h"

#include "src/function/string/include/vector_string_operations.h"

namespace graphflow {
namespace function {

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    if (isExpressionBinary(expressionType)) {
        return bindBinaryExecFunction(expressionType, children);
    } else if (isExpressionUnary(expressionType)) {
        return bindUnaryExecFunction(expressionType, children);
    }
    assert(false);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindAbsExecFunction(
    const expression_vector& children) {
    validateNumParameters(ABS_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Abs>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindFloorExecFunction(
    const expression_vector& children) {
    validateNumParameters(FLOOR_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Floor>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindCeilExecFunction(
    const expression_vector& children) {
    validateNumParameters(CEIL_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Ceil>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindSinExecFunction(
    const expression_vector& children) {
    validateNumParameters(SIN_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Sin>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindCosExecFunction(
    const expression_vector& children) {
    validateNumParameters(COS_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Cos>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindTanExecFunction(
    const expression_vector& children) {
    validateNumParameters(TAN_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Tan>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindCotExecFunction(
    const expression_vector& children) {
    validateNumParameters(COT_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Cot>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindAsinExecFunction(
    const expression_vector& children) {
    validateNumParameters(ASIN_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Asin>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindAcosExecFunction(
    const expression_vector& children) {
    validateNumParameters(ACOS_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Acos>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindAtanExecFunction(
    const expression_vector& children) {
    validateNumParameters(ATAN_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Atan>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindEvenExecFunction(
    const expression_vector& children) {
    validateNumParameters(EVEN_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Even>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindFactorialExecFunction(
    const expression_vector& children) {
    validateNumParameters(FACTORIAL_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Factorial>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindSignExecFunction(
    const expression_vector& children) {
    validateNumParameters(SIGN_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Sign>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindSqrtExecFunction(
    const expression_vector& children) {
    validateNumParameters(SQRT_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Sqrt>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindCbrtExecFunction(
    const expression_vector& children) {
    validateNumParameters(CBRT_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Cbrt>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindGammaExecFunction(
    const expression_vector& children) {
    validateNumParameters(GAMMA_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Gamma>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindLgammaExecFunction(
    const expression_vector& children) {
    validateNumParameters(LGAMMA_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Lgamma>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindLnExecFunction(
    const expression_vector& children) {
    validateNumParameters(LN_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Ln>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindLogExecFunction(
    const expression_vector& children) {
    validateNumParameters(LOG_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Log>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindLog2ExecFunction(
    const expression_vector& children) {
    validateNumParameters(LOG2_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Log2>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindDegreesExecFunction(
    const expression_vector& children) {
    validateNumParameters(DEGREES_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Degrees>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindRadiansExecFunction(
    const expression_vector& children) {
    validateNumParameters(RADIANS_FUNC_NAME, children.size(), 1);
    return bindUnaryExecFunction<operation::Radians>(children[0]->dataType);
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindAtan2ExecFunction(
    const expression_vector& children) {
    validateNumParameters(ATAN2_FUNC_NAME, children.size(), 2);
    validateParameterType(ATAN2_FUNC_NAME, *children[0], {INT64, DOUBLE, UNSTRUCTURED});
    validateParameterType(ATAN2_FUNC_NAME, *children[1], {INT64, DOUBLE, UNSTRUCTURED});
    switch (children[0]->dataType.typeID) {
    case INT64:
        switch (children[1]->dataType.typeID) {
        case INT64:
            return make_pair(
                BinaryExecFunction<int64_t, int64_t, double_t, operation::Atan2>, DataType(DOUBLE));

        case DOUBLE:
            return make_pair(BinaryExecFunction<int64_t, double_t, double_t, operation::Atan2>,
                DataType(DOUBLE));
        }
    case DOUBLE:
        switch (children[1]->dataType.typeID) {
        case INT64:
            return make_pair(BinaryExecFunction<double_t, int64_t, double_t, operation::Atan2>,
                DataType(DOUBLE));

        case DOUBLE:
            return make_pair(BinaryExecFunction<double_t, double_t, double_t, operation::Atan2>,
                DataType(DOUBLE));
        }

    case UNSTRUCTURED:
        assert(children[1]->dataType.typeID == UNSTRUCTURED);
        return make_pair(
            BinaryExecFunction<Value, Value, Value, operation::Atan2>, DataType(UNSTRUCTURED));
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindRoundExecFunction(
    const expression_vector& children) {
    validateNumParameters(ROUND_FUNC_NAME, children.size(), 2);
    validateParameterType(ROUND_FUNC_NAME, *children[0], {DOUBLE, UNSTRUCTURED});
    validateParameterType(ROUND_FUNC_NAME, *children[1], {INT64, UNSTRUCTURED});
    switch (children[0]->dataType.typeID) {
    case DOUBLE:
        assert(children[1]->dataType.typeID == INT64);
        return make_pair(
            BinaryExecFunction<double_t, int64_t, double_t, operation::Round>, DataType(DOUBLE));
    case UNSTRUCTURED:
        assert(children[1]->dataType.typeID == UNSTRUCTURED);
        return make_pair(
            BinaryExecFunction<Value, Value, Value, operation::Round>, DataType(UNSTRUCTURED));
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindXorExecFunction(
    const expression_vector& children) {
    validateNumParameters(ROUND_FUNC_NAME, children.size(), 2);
    validateParameterType(ROUND_FUNC_NAME, *children[0], {INT64, UNSTRUCTURED});
    validateParameterType(ROUND_FUNC_NAME, *children[1], {INT64, UNSTRUCTURED});
    switch (children[0]->dataType.typeID) {
    case INT64:
        assert(children[1]->dataType.typeID == INT64);
        return make_pair(
            BinaryExecFunction<int64_t, int64_t, int64_t, operation::BitWiseXor>, DataType(INT64));
    case UNSTRUCTURED:
        assert(children[1]->dataType.typeID == UNSTRUCTURED);
        return make_pair(
            BinaryExecFunction<Value, Value, Value, operation::BitWiseXor>, DataType(UNSTRUCTURED));
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindBinaryExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 2);
    switch (children[0]->dataType.typeID) {
    case STRING: {
        return bindStringArithmeticExecFunction(expressionType, children);
    }
    case DATE: {
        return bindDateArithmeticExecFunction(expressionType, children);
    }
    case TIMESTAMP: {
        return bindTimestampArithmeticExecFunction(expressionType, children);
    }
    case INTERVAL: {
        return bindIntervalArithmeticExecFunction(expressionType, children);
    }
    default: {
        return bindNumericalArithmeticExecFunction(expressionType, children);
    }
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindStringArithmeticExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(expressionType == ADD);
    validateParameterType(expressionTypeToString(expressionType), *children[1], STRING);
    return make_pair(
        VectorStringOperations::bindExecFunction(expressionType, children), DataType(STRING));
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindDateArithmeticExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    switch (expressionType) {
    case ADD: {
        validateParameterType(expressionTypeToString(expressionType), *children[1],
            unordered_set<DataTypeID>{INT64, INTERVAL});
        switch (children[1]->dataType.typeID) {
        case INT64: { // date + int → date
            return make_pair(
                BinaryExecFunction<date_t, int64_t, date_t, operation::Add>, DataType(DATE));
        }
        case INTERVAL: { // date + interval → date
            return make_pair(
                BinaryExecFunction<date_t, interval_t, date_t, operation::Add>, DataType(DATE));
        }
        default:
            assert(false);
        }
    }
    case SUBTRACT: {
        validateParameterType(expressionTypeToString(expressionType), *children[1],
            unordered_set<DataTypeID>{DATE, INT64, INTERVAL});
        switch (children[1]->dataType.typeID) {
        case DATE: { // date - date → integer
            return make_pair(
                BinaryExecFunction<date_t, date_t, int64_t, operation::Subtract>, DataType(INT64));
        }
        case INT64: { // date - integer → date
            return make_pair(
                BinaryExecFunction<date_t, int64_t, date_t, operation::Subtract>, DataType(DATE));
        }
        case INTERVAL: { // date - interval → date
            return make_pair(BinaryExecFunction<date_t, interval_t, date_t, operation::Subtract>,
                DataType(DATE));
        }
        default:
            assert(false);
        }
    }
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindTimestampArithmeticExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    switch (expressionType) {
    case ADD: {
        validateParameterType(expressionTypeToString(expressionType), *children[1], INTERVAL);
        switch (children[1]->dataType.typeID) {
        case INTERVAL: { // timestamp + interval → timestamp
            return make_pair(
                BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Add>,
                DataType(TIMESTAMP));
        }
        default:
            assert(false);
        }
    }
    case SUBTRACT: {
        validateParameterType(expressionTypeToString(expressionType), *children[1],
            unordered_set<DataTypeID>{TIMESTAMP, INTERVAL});
        switch (children[1]->dataType.typeID) {
        case TIMESTAMP: { // timestamp - timestamp → interval
            return make_pair(
                BinaryExecFunction<timestamp_t, timestamp_t, interval_t, operation::Subtract>,
                DataType(INTERVAL));
        }
        case INTERVAL: { // timestamp - interval → timestamp
            return make_pair(
                BinaryExecFunction<timestamp_t, interval_t, timestamp_t, operation::Subtract>,
                DataType(TIMESTAMP));
        }
        default:
            assert(false);
        }
    }
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindIntervalArithmeticExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    switch (expressionType) {
    case ADD: {
        validateParameterType(expressionTypeToString(expressionType), *children[1], INTERVAL);
        switch (children[1]->dataType.typeID) {
        case INTERVAL: { // interval +interval → interval
            return make_pair(BinaryExecFunction<interval_t, interval_t, interval_t, operation::Add>,
                DataType(INTERVAL));
        }
        default:
            assert(false);
        }
    }
    case SUBTRACT: {
        validateParameterType(expressionTypeToString(expressionType), *children[1], INTERVAL);
        switch (children[1]->dataType.typeID) {
        case INTERVAL: { // interval - interval → interval
            return make_pair(
                BinaryExecFunction<interval_t, interval_t, interval_t, operation::Subtract>,
                DataType(INTERVAL));
        }
        default:
            assert(false);
        }
    }
    case DIVIDE: {
        validateParameterType(expressionTypeToString(expressionType), *children[1], INT64);
        switch (children[1]->dataType.typeID) {
        case INT64: { // interval / int → interval
            return make_pair(BinaryExecFunction<interval_t, int64_t, interval_t, operation::Divide>,
                DataType(INTERVAL));
        }
        default:
            assert(false);
        }
    }
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindNumericalArithmeticExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    auto leftType = children[0]->dataType;
    auto rightType = children[1]->dataType;
    validateParameterType(
        expressionTypeToString(expressionType), *children[0], {INT64, DOUBLE, UNSTRUCTURED});
    validateParameterType(
        expressionTypeToString(expressionType), *children[1], {INT64, DOUBLE, UNSTRUCTURED});
    switch (leftType.typeID) {
    case INT64: {
        switch (rightType.typeID) {
        case INT64: {
            return make_pair(
                bindNumericalArithmeticExecFunction<int64_t, int64_t, int64_t>(expressionType),
                DataType(INT64));
        }
        case DOUBLE: {
            return make_pair(
                bindNumericalArithmeticExecFunction<int64_t, double_t, double_t>(expressionType),
                DataType(DOUBLE));
        }
        default:
            assert(false);
        }
    }
    case DOUBLE: {
        switch (rightType.typeID) {
        case INT64: {
            return make_pair(
                bindNumericalArithmeticExecFunction<double_t, int64_t, double_t>(expressionType),
                DataType(DOUBLE));
        }
        case DOUBLE: {
            return make_pair(
                bindNumericalArithmeticExecFunction<double_t, double_t, double_t>(expressionType),
                DataType(DOUBLE));
        }
        default:
            assert(false);
        }
    }
    case UNSTRUCTURED: {
        assert(rightType.typeID == UNSTRUCTURED);
        return make_pair(bindNumericalArithmeticExecFunction<Value, Value, Value>(expressionType),
            DataType(UNSTRUCTURED));
    }
    default:
        assert(false);
    }
}

template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE>
scalar_exec_func VectorArithmeticOperations::bindNumericalArithmeticExecFunction(
    ExpressionType expressionType) {
    switch (expressionType) {
    case ADD: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Add>;
    }
    case SUBTRACT: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Subtract>;
    }
    case MULTIPLY: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Multiply>;
    }
    case DIVIDE: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Divide>;
    }
    case MODULO: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Modulo>;
    }
    case POWER: {
        return BinaryExecFunction<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, operation::Power>;
    }
    default:
        assert(false);
    }
}

pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindUnaryExecFunction(
    ExpressionType expressionType, const expression_vector& children) {
    assert(children.size() == 1 && expressionType == NEGATE);
    return bindUnaryExecFunction<operation::Negate>(children[0]->dataType);
}

template<typename FUNC>
pair<scalar_exec_func, DataType> VectorArithmeticOperations::bindUnaryExecFunction(
    const DataType& operandType) {
    if constexpr (operation::isFuncInputNumericOutputDouble<FUNC>()) {
        switch (operandType.typeID) {
        case INT64: {
            return make_pair(UnaryExecFunction<int64_t, double_t, FUNC>, DataType(DOUBLE));
        }
        case DOUBLE: {
            return make_pair(UnaryExecFunction<double_t, double_t, FUNC>, DataType(DOUBLE));
        }
        case UNSTRUCTURED: {
            return make_pair(UnaryExecFunction<Value, Value, FUNC>, DataType(UNSTRUCTURED));
        }
        default:
            assert(false);
        }
    } else if constexpr (operation::isFuncInputNumericOutputInt64<FUNC>()) {
        switch (operandType.typeID) {
        case INT64: {
            return make_pair(UnaryExecFunction<int64_t, int64_t, FUNC>, DataType(INT64));
        }
        case DOUBLE: {
            return make_pair(UnaryExecFunction<double_t, int64_t, FUNC>, DataType(INT64));
        }
        case UNSTRUCTURED: {
            return make_pair(UnaryExecFunction<Value, Value, FUNC>, DataType(UNSTRUCTURED));
        }
        default:
            assert(false);
        }
    } else {
        switch (operandType.typeID) {
        case INT64: {
            return make_pair(UnaryExecFunction<int64_t, int64_t, FUNC>, DataType(INT64));
        }
        case DOUBLE: {
            return make_pair(UnaryExecFunction<double_t, double_t, FUNC>, DataType(DOUBLE));
        }
        case UNSTRUCTURED: {
            return make_pair(UnaryExecFunction<Value, Value, FUNC>, DataType(UNSTRUCTURED));
        }
        default:
            assert(false);
        }
    }
}

} // namespace function
} // namespace graphflow
