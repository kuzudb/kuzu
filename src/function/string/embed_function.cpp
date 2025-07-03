#include "binder/expression/expression_util.h"
#include "expression_evaluator/expression_evaluator_utils.h"
#include "function/string/functions/base_regexp_function.h"
#include "function/string/vector_string_functions.h"
#include "re2.h"

namespace kuzu {
namespace function {

using namespace common;

struct EmbedFunction {
    static void operation(common::ku_string_t& str, uint8* array, ValueVector* strVector,
        ValueVector* arrayVector) {
        auto result = openApi.transform(str.getAsString());
    }
};

function_set RegexpFullMatchFunction::getFunctionSet() {
    function_set functionSet;
    auto scalarFunc = make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::ARRAY,
        ScalarFunction::BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, EmbedFunction>);
    scalarFunc->bindFunc = regexFullMatchBindFunc;
    functionSet.emplace_back(std::move(scalarFunc));
    return functionSet;
}

} // namespace function
} // namespace kuzu
