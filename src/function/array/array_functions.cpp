#include "common/exception/binder.h"
#include "function/array/functions/array_cosine_similarity.h"
#include "function/array/functions/array_cross_product.h"
#include "function/array/functions/array_distance.h"
#include "function/array/functions/array_inner_product.h"
#include "function/array/vector_array_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

std::unique_ptr<FunctionBindData> ArrayCrossProductBindFunc(
    const binder::expression_vector& arguments, Function* function) {
    const auto& leftType = arguments[0]->dataType;
    const auto& rightType = arguments[1]->dataType;
    if (leftType != rightType) {
        throw BinderException(
            stringFormat("{} requires both arrays to have the same element type and size of 3",
                ArrayCrossProductFunction::name));
    }
    scalar_func_exec_t execFunc;
    switch (ArrayType::getChildType(leftType).getLogicalTypeID()) {
    case LogicalTypeID::INT128:
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
            list_entry_t, ArrayCrossProduct<int128_t>>;
        break;
    case LogicalTypeID::INT64:
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
            list_entry_t, ArrayCrossProduct<int64_t>>;
        break;
    case LogicalTypeID::INT32:
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
            list_entry_t, ArrayCrossProduct<int32_t>>;
        break;
    case LogicalTypeID::INT16:
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
            list_entry_t, ArrayCrossProduct<int16_t>>;
        break;
    case LogicalTypeID::INT8:
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
            list_entry_t, ArrayCrossProduct<int8_t>>;
        break;
    case LogicalTypeID::FLOAT:
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
            list_entry_t, ArrayCrossProduct<float>>;
        break;
    case LogicalTypeID::DOUBLE:
        execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
            list_entry_t, ArrayCrossProduct<double>>;
        break;
    default:
        throw BinderException{
            stringFormat("{} can only be applied on array of floating points or integers",
                ArrayCrossProductFunction::name)};
    }
    ku_dynamic_cast<Function*, ScalarFunction*>(function)->execFunc = execFunc;
    auto resultType = LogicalType::ARRAY(ArrayType::getChildType(leftType).copy(),
        ArrayType::getNumElements(leftType));
    return FunctionBindData::getSimpleBindData(arguments, std::move(resultType));
}

function_set ArrayCrossProductFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{
            LogicalTypeID::ARRAY,
            LogicalTypeID::ARRAY,
        },
        LogicalTypeID::ARRAY, nullptr, nullptr, ArrayCrossProductBindFunc));
    return result;
}

static LogicalType getChildType(const LogicalType& type) {
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::ARRAY:
        return ArrayType::getChildType(type).copy();
    case LogicalTypeID::LIST:
        return ListType::getChildType(type).copy();
        // LCOV_EXCL_START
    default:
        throw BinderException(stringFormat(
            "Cannot retrieve child type of type {}. LIST or ARRAY is expected.", type.toString()));
        // LCOV_EXCL_STOP
    }
}

static void validateChildType(const LogicalType& type, const std::string& functionName) {
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
        return;
    default:
        throw BinderException(
            stringFormat("{} requires argument type to be FLOAT[] or DOUBLE[].", functionName));
    }
}

static LogicalType validateArrayFunctionParameters(const LogicalType& leftType,
    const LogicalType& rightType, const std::string& functionName) {
    auto leftChildType = getChildType(leftType);
    auto rightChildType = getChildType(rightType);
    validateChildType(leftChildType, functionName);
    validateChildType(rightChildType, functionName);
    if (leftType.getLogicalTypeID() == common::LogicalTypeID::ARRAY) {
        return leftType.copy();
    } else if (rightType.getLogicalTypeID() == common::LogicalTypeID::ARRAY) {
        return rightType.copy();
    }
    throw BinderException(
        stringFormat("{} requires at least one argument to be ARRAY but all parameters are LIST.",
            functionName));
}

template<typename OPERATION, typename RESULT>
static scalar_func_exec_t getBinaryArrayExecFuncSwitchResultType() {
    auto execFunc =
        ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t, RESULT, OPERATION>;
    return execFunc;
}

template<typename OPERATION>
scalar_func_exec_t getScalarExecFunc(LogicalType type) {
    scalar_func_exec_t execFunc;
    switch (ArrayType::getChildType(type).getLogicalTypeID()) {
    case LogicalTypeID::FLOAT:
        execFunc = getBinaryArrayExecFuncSwitchResultType<OPERATION, float>();
        break;
    case LogicalTypeID::DOUBLE:
        execFunc = getBinaryArrayExecFuncSwitchResultType<OPERATION, double>();
        break;
    default:
        KU_UNREACHABLE;
    }
    return execFunc;
}

template<typename OPERATION>
std::unique_ptr<FunctionBindData> arrayTemplateBindFunc(std::string functionName,
    const binder::expression_vector& arguments, Function* function) {
    const auto& leftType = arguments[0]->dataType;
    const auto& rightType = arguments[1]->dataType;
    auto paramType = validateArrayFunctionParameters(leftType, rightType, functionName);
    function->ptrCast<ScalarFunction>()->execFunc =
        std::move(getScalarExecFunc<OPERATION>(paramType.copy()));
    auto bindData = std::make_unique<FunctionBindData>(ArrayType::getChildType(paramType).copy());
    std::vector<LogicalType> paramTypes;
    for (auto& _ : arguments) {
        (void)_;
        bindData->paramTypes.push_back(paramType.copy());
    }
    return bindData;
}

template<typename OPERATION>
function_set templateGetFunctionSet(const std::string& functionName) {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(functionName,
        std::vector<LogicalTypeID>{
            LogicalTypeID::ARRAY,
            LogicalTypeID::ARRAY,
        },
        LogicalTypeID::ANY, nullptr, nullptr,
        std::bind(arrayTemplateBindFunc<OPERATION>, functionName, std::placeholders::_1,
            std::placeholders::_2)));
    return result;
}

function_set ArrayCosineSimilarityFunction::getFunctionSet() {
    return templateGetFunctionSet<ArrayCosineSimilarity>(name);
}

function_set ArrayDistanceFunction::getFunctionSet() {
    return templateGetFunctionSet<ArrayDistance>(name);
}

function_set ArrayInnerProductFunction::getFunctionSet() {
    return templateGetFunctionSet<ArrayInnerProduct>(name);
}

function_set ArrayDotProductFunction::getFunctionSet() {
    return templateGetFunctionSet<ArrayInnerProduct>(name);
}

} // namespace function
} // namespace kuzu
