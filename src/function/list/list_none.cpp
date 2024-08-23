#include "function/list/vector_list_functions.h"
#include "function/scalar_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

bool noneHandler(uint64_t numSelectedValues, uint64_t /*originalSize*/) {
    return numSelectedValues == 0;
}

function_set ListNoneFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::LIST, LogicalTypeID::ANY}, LogicalTypeID::BOOL,
        std::bind(execQuantifierFunc, noneHandler, std::placeholders::_1, std::placeholders::_2,
            std::placeholders::_3),
        bindQuantifierFunc);
    function->isListLambda = true;
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
