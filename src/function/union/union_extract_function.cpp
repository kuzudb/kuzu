#include "function/scalar_function.h"
#include "function/struct/vector_struct_functions.h"
#include "function/union/vector_union_functions.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set UnionExtractFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::UNION, LogicalTypeID::STRING}, LogicalTypeID::ANY,
        nullptr, nullptr, StructExtractFunctions::compileFunc, StructExtractFunctions::bindFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
