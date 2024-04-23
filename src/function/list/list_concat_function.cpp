#include "function/list/functions/list_concat_function.h"

#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

std::unique_ptr<FunctionBindData> ListConcatFunction::bindFunc(
    const binder::expression_vector& arguments, Function* /*function*/) {
    if (arguments[0]->getDataType() != arguments[1]->getDataType()) {
        throw BinderException(ExceptionMessage::listFunctionIncompatibleChildrenType(name,
            arguments[0]->getDataType().toString(), arguments[1]->getDataType().toString()));
    }
    return FunctionBindData::getSimpleBindData(arguments, arguments[0]->getDataType());
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
