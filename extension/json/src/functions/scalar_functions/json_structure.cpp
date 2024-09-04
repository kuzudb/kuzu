#include "function/scalar_function.h"
#include "json_scalar_functions.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    const auto& param = parameters[0];
    for (auto selectedPos = 0u; selectedPos < result.state->getSelVector().getSelSize();
         ++selectedPos) {
        auto resultPos = result.state->getSelVector()[selectedPos];
        auto paramPos = param->state->getSelVector()[param->state->isFlat() ? 0 : selectedPos];
        auto isNull = parameters[0]->isNull(paramPos);
        result.setNull(resultPos, isNull);
        if (!isNull) {
            auto paramStr = param->getValue<ku_string_t>(paramPos).getAsString();
            auto schema = jsonSchema(stringToJson(paramStr));
            result.setNull(resultPos, false /* isNull */);
            StringVector::addString(&result, resultPos, schema.toString());
        }
    }
}

function_set JsonStructureFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::STRING, execFunc));
    return result;
}

} // namespace json_extension
} // namespace kuzu
