#include "common/exception/binder.h"
#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

using namespace common;

static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
    Function* /*function*/) {
    if (arguments[1]->expressionType != ExpressionType::LAMBDA) {
        throw BinderException(stringFormat(
            "The second argument of LIST_TRANSFORM should be a lambda expression but got {}.",
            ExpressionTypeUtil::toString(arguments[1]->expressionType)));
    }
    std::vector<LogicalType> paramTypes;
    paramTypes.push_back(arguments[0]->getDataType().copy());
    paramTypes.push_back(arguments[1]->getDataType().copy());
    return std::make_unique<FunctionBindData>(std::move(paramTypes),
        LogicalType::LIST(arguments[1]->getDataType().copy()));
}

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>&, ValueVector&, void*) {}

function_set ListTransformFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::LIST,
        execFunc);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
