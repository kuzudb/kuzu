#include "function/list/functions/list_concat_function.h"

#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void ListConcat::operation(common::list_entry_t& left, common::list_entry_t& right,
    common::list_entry_t& result, common::ValueVector& leftVector, common::ValueVector& rightVector,
    common::ValueVector& resultVector) {
    result = common::ListVector::addList(&resultVector, left.size + right.size);
    auto resultDataVector = common::ListVector::getDataVector(&resultVector);
    auto resultPos = result.offset;
    auto leftDataVector = common::ListVector::getDataVector(&leftVector);
    auto leftPos = left.offset;
    for (auto i = 0u; i < left.size; i++) {
        resultDataVector->copyFromVectorData(resultPos++, leftDataVector, leftPos++);
    }
    auto rightDataVector = common::ListVector::getDataVector(&rightVector);
    auto rightPos = right.offset;
    for (auto i = 0u; i < right.size; i++) {
        resultDataVector->copyFromVectorData(resultPos++, rightDataVector, rightPos++);
    }
}

static void resolveEmptyList(const ScalarBindFuncInput& input, std::vector<LogicalType>& types) {
    auto isArg0Empty = binder::ExpressionUtil::isEmptyList(*input.arguments[0]);
    auto isArg1Empty = binder::ExpressionUtil::isEmptyList(*input.arguments[1]);
    if (isArg0Empty || isArg1Empty) {
        auto& typeToUse = input.arguments[isArg0Empty ? 1 : 0]->getDataType();
        types[0] = typeToUse.copy();
        types[1] = typeToUse.copy();
    }
}

static void resolveNullList(const ScalarBindFuncInput& input, std::vector<LogicalType>& types) {
    auto isArg0AnyType = input.arguments[0]->dataType.getLogicalTypeID() == LogicalTypeID::ANY;
    auto isArg1AnyType = input.arguments[1]->dataType.getLogicalTypeID() == LogicalTypeID::ANY;
    LogicalType targetType;
    if (isArg0AnyType && isArg1AnyType) {
        targetType = LogicalType::LIST(LogicalType::INT64());
    } else if (isArg0AnyType || isArg1AnyType) {
        targetType = input.arguments[isArg0AnyType ? 1 : 0]->getDataType().copy();
    } else {
        return;
    }
    types[0] = targetType.copy();
    types[1] = targetType.copy();
}

std::unique_ptr<FunctionBindData> ListConcatFunction::bindFunc(const ScalarBindFuncInput& input) {
    std::vector<LogicalType> types;
    types.push_back(input.arguments[0]->getDataType().copy());
    types.push_back(input.arguments[1]->getDataType().copy());
    resolveEmptyList(input, types);
    resolveNullList(input, types);
    if (types[0] != types[1]) {
        throw BinderException(ExceptionMessage::listFunctionIncompatibleChildrenType(name,
            types[0].toString(), types[1].toString()));
    }
    return std::make_unique<FunctionBindData>(std::move(types), types[0].copy());
}

function_set ListConcatFunction::getFunctionSet() {
    function_set result;
    auto execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, ListConcat>;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::LIST,
        execFunc);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
