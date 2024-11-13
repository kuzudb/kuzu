#include "function/scalar_function.h"
#include "json_creation_functions.h"
#include "json_type.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    ValueVector& result, void* /*dataPtr*/) {
    KU_ASSERT(parameters.size() == 1);
    result.resetAuxiliaryBuffer();
    for (auto i = 0u; i < result.state->getSelVector().getSelSize(); ++i) {
        auto inputPos = parameters[0]->state->getSelVector()[i];
        auto resultPos = result.state->getSelVector()[i];
        result.setNull(resultPos, parameters[0]->isNull(inputPos));
        if (!parameters[0]->isNull(inputPos)) {
            StringVector::addString(&result, resultPos,
                jsonToString(jsonify(*parameters[0], inputPos)));
        }
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    return FunctionBindData::getSimpleBindData(input.arguments, JsonType::getJsonType());
}

function_set ToJsonFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY}, LogicalTypeID::STRING, execFunc, bindFunc));
    return result;
}

} // namespace json_extension
} // namespace kuzu
