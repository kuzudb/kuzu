#include "math.h"

#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/type_utils.h"
#include "common/vector/value_vector.h"
#include "function/list/functions/list_function_utils.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"
#include <simsimd.h>

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ListCosineSimilarity {
    template<std::floating_point T>
    static void operation(common::list_entry_t& left, common::list_entry_t& right, T& result,
        common::ValueVector& leftVector, common::ValueVector& rightVector,
        common::ValueVector& /*resultVector*/) {
        auto leftElements = (T*)common::ListVector::getListValues(&leftVector, left);
        auto rightElements = (T*)common::ListVector::getListValues(&rightVector, right);
        if (left.size != right.size) {
            throw BinderException(stringFormat("LIST_COSINE_SIMILARITY requires both arguments to "
                                               "be in same size: left : {} ; right : {}",
                left.size, right.size));
        }
        KU_ASSERT(left.size == right.size);
        simsimd_distance_t tmpResult = 0.0;
        static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);
        if constexpr (std::is_same_v<T, float>) {
            simsimd_cos_f32(leftElements, rightElements, left.size, &tmpResult);
        } else {
            simsimd_cos_f64(leftElements, rightElements, left.size, &tmpResult);
        }
        result = 1.0 - tmpResult;
    }
};

struct ListCosineDistance {
    template<std::floating_point T>
    static void operation(common::list_entry_t& left, common::list_entry_t& right, T& result,
        common::ValueVector& leftVector, common::ValueVector& rightVector,
        common::ValueVector& /*resultVector*/) {
        auto leftElements = (T*)common::ListVector::getListValues(&leftVector, left);
        auto rightElements = (T*)common::ListVector::getListValues(&rightVector, right);
        if (left.size != right.size) {
            throw BinderException(stringFormat("LIST_COSINE_DISTANCE requires both arguments to be "
                                               "in same size: left : {} ; right : {}",
                left.size, right.size));
        }
        KU_ASSERT(left.size == right.size);
        simsimd_distance_t tmpResult = 0.0;
        static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);
        if constexpr (std::is_same_v<T, float>) {
            simsimd_cos_f32(leftElements, rightElements, left.size, &tmpResult);
        } else {
            simsimd_cos_f64(leftElements, rightElements, left.size, &tmpResult);
        }
        result = tmpResult;
    }
};

struct ListDistance {
    template<std::floating_point T>
    static void operation(common::list_entry_t& left, common::list_entry_t& right, T& result,
        common::ValueVector& leftVector, common::ValueVector& rightVector,
        common::ValueVector& /*resultVector*/) {
        auto leftElements = (T*)common::ListVector::getListValues(&leftVector, left);
        auto rightElements = (T*)common::ListVector::getListValues(&rightVector, right);
        if (left.size != right.size) {
            throw BinderException(stringFormat(
                "LIST_DISTANCE requires both arguments to be in same size: left : {} ; right : {}",
                left.size, right.size));
        }
        KU_ASSERT(left.size == right.size);
        simsimd_distance_t tmpResult = 0.0;
        static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);
        if constexpr (std::is_same_v<T, float>) {
            simsimd_l2sq_f32(leftElements, rightElements, left.size, &tmpResult);
        } else {
            simsimd_l2sq_f64(leftElements, rightElements, left.size, &tmpResult);
        }
        result = std::sqrt(tmpResult);
    }
};

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

static LogicalType validateListFunctionParameters(const LogicalType& leftType,
    const LogicalType& rightType, const std::string& functionName) {
    if ((leftType.getPhysicalType() != common::PhysicalTypeID::LIST) ||
        (rightType.getPhysicalType() != common::PhysicalTypeID::LIST)) {
        throw BinderException(
            stringFormat("Function {} did not receive correct arguments", functionName));
    }
    const auto& leftChildType = ListType::getChildType(leftType);
    const auto& rightChildType = ListType::getChildType(rightType);
    validateChildType(leftChildType, functionName);
    validateChildType(rightChildType, functionName);
    if (leftType.getLogicalTypeID() == common::LogicalTypeID::LIST) {
        return leftType.copy();
    } else if (rightType.getLogicalTypeID() == common::LogicalTypeID::LIST) {
        return rightType.copy();
    }
    throw BinderException(
        stringFormat("{} requires at least one argument to be LIST.", functionName));
}

template<typename OPERATION, typename RESULT>
static scalar_func_exec_t getBinaryListExecFuncSwitchResultType() {
    auto execFunc =
        ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t, RESULT, OPERATION>;
    return execFunc;
}

template<typename OPERATION>
scalar_func_exec_t getScalarExecFunc(LogicalType type) {
    scalar_func_exec_t execFunc;
    switch (ListType::getChildType(type).getLogicalTypeID()) {
    case LogicalTypeID::FLOAT:
        execFunc = getBinaryListExecFuncSwitchResultType<OPERATION, float>();
        break;
    case LogicalTypeID::DOUBLE:
        execFunc = getBinaryListExecFuncSwitchResultType<OPERATION, double>();
        break;
    default:
        KU_UNREACHABLE;
    }
    return execFunc;
}

template<typename OPERATION>
static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    std::vector<LogicalType> types;
    types.push_back(input.arguments[0]->getDataType().copy());
    types.push_back(input.arguments[1]->getDataType().copy());
    auto paramType = validateListFunctionParameters(types[0], types[1], input.definition->name);
    input.definition->ptrCast<ScalarFunction>()->execFunc =
        std::move(getScalarExecFunc<OPERATION>(paramType.copy()));
    auto bindData = std::make_unique<FunctionBindData>(ListType::getChildType(paramType).copy());
    std::vector<LogicalType> paramTypes;
    for (auto& _ : input.arguments) {
        (void)_;
        bindData->paramTypes.push_back(paramType.copy());
    }
    return bindData;
}
template<typename OPERATION>
function_set templateGetFunctionSet(const std::string& name) {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::ANY);
    function->bindFunc = bindFunc<OPERATION>;
    result.push_back(std::move(function));
    return result;
}

function_set ListCosineSimilarityFunction::getFunctionSet() {
    return templateGetFunctionSet<ListCosineSimilarity>(name);
}

function_set ListCosineDistanceFunction::getFunctionSet() {
    return templateGetFunctionSet<ListCosineDistance>(name);
}

function_set ListDistanceFunction::getFunctionSet() {
    return templateGetFunctionSet<ListDistance>(name);
}

} // namespace function
} // namespace kuzu
