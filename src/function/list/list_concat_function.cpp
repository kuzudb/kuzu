#include "function/list/functions/list_concat_function.h"

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

std::unique_ptr<FunctionBindData> ListConcatFunction::bindFunc(ScalarBindFuncInput input) {
    if (input.arguments[0]->getDataType() != input.arguments[1]->getDataType()) {
        throw BinderException(ExceptionMessage::listFunctionIncompatibleChildrenType(name,
            input.arguments[0]->getDataType().toString(),
            input.arguments[1]->getDataType().toString()));
    }
    return FunctionBindData::getSimpleBindData(input.arguments, input.arguments[0]->getDataType());
}

function_set ListConcatFunction::getFunctionSet() {
    function_set result;
    auto execFunc = ScalarFunction::BinaryExecListStructFunction<list_entry_t, list_entry_t,
        list_entry_t, ListConcat>;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::LIST}, LogicalTypeID::LIST,
        execFunc, nullptr, bindFunc));
    return result;
}

} // namespace function
} // namespace kuzu
