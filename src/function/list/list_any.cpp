#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

namespace kuzu {
namespace function {

using namespace kuzu::common;

bool anyHandler(uint64_t numSelectedValues, uint64_t /*originalSize*/) {
    return numSelectedValues > 0;
}

function_set ListAnyFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::BOOL,
        std::bind(execQuantifierFunc, anyHandler, std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3),
        bindQuantifierFunc);
    function->isListLambda = true;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
