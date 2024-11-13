#include "function/scalar_function.h"
#include "json_scalar_functions.h"
#include "json_type.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
        ++selectedPos) {
        auto inputPos = parameters[0]->state->getSelVector()[selectedPos];
        auto resultPos = result.state->getSelVector()[selectedPos];
        auto isNull = parameters[0]->isNull(inputPos);
        result.setNull(resultPos, isNull);
        if (!isNull) {
            StringVector::addString(&result, resultPos,
                jsonToString(
                    stringToJson(parameters[0]->getValue<ku_string_t>(inputPos).getAsString())));
        }
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    return FunctionBindData::getSimpleBindData(input.arguments, JsonType::getJsonType());
}

function_set MinifyJsonFunction::getFunctionSet() {
    function_set result;
    result.push_back(
        std::make_unique<ScalarFunction>(name, std::vector<LogicalTypeID>{LogicalTypeID::STRING},
            LogicalTypeID::STRING, execFunc, bindFunc));
    return result;
}

} // namespace json_extension
} // namespace kuzu
