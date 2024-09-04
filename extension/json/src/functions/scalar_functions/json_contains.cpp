#include "function/scalar_function.h"
#include "json_scalar_functions.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    const auto& param1 = parameters[0];
    const auto& param2 = parameters[1];
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
         ++selectedPos) {
        auto resultPos = result.state->getSelVector()[selectedPos];
        auto param1Pos = param1->state->getSelVector()[param1->state->isFlat() ? 0 : selectedPos];
        auto param2Pos = param2->state->getSelVector()[param2->state->isFlat() ? 0 : selectedPos];
        auto isNull = param1->isNull(param1Pos) || param2->isNull(param2Pos);
        result.setNull(resultPos, isNull);
        if (!isNull) {
            auto param1Str = param1->getValue<ku_string_t>(param1Pos).getAsString();
            auto param2Str = param2->getValue<ku_string_t>(param2Pos).getAsString();
            result.setValue<bool>(resultPos,
                jsonContains(stringToJson(param1Str), stringToJson(param2Str)));
        }
    }
}

function_set JsonContainsFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL, execFunc));
    return result;
}

} // namespace json_extension
} // namespace kuzu
