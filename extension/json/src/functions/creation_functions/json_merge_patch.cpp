#include "function/scalar_function.h"
#include "json_creation_functions.h"
#include "json_type.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    const auto& param1 = *parameters[0];
    const auto& param2 = *parameters[1];
    const auto& param1SelVector = *parameterSelVectors[0];
    const auto& param2SelVector = *parameterSelVectors[1];
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {
        auto resultPos = (*resultSelVector)[selectedPos];
        auto param1Pos = param1SelVector[param1.state->isFlat() ? 0 : selectedPos];
        auto param2Pos = param2SelVector[param2.state->isFlat() ? 0 : selectedPos];
        auto isNull = param1.isNull(param1Pos) || param2.isNull(param2Pos);
        result.setNull(resultPos, isNull);
        if (!isNull) {
            auto param1Str = param1.getValue<ku_string_t>(param1Pos).getAsString();
            auto param2Str = param2.getValue<ku_string_t>(param2Pos).getAsString();
            StringVector::addString(&result, resultPos,
                jsonToString(mergeJson(stringToJson(param1Str), stringToJson(param2Str))));
        }
    }
}

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    std::vector<LogicalType> types;
    types.emplace_back(input.definition->parameterTypeIDs[0]);
    types.emplace_back(input.definition->parameterTypeIDs[1]);
    return std::make_unique<FunctionBindData>(std::move(types), JsonType::getJsonType());
}

function_set JsonMergePatchFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING, execFunc);
    func->bindFunc = bindFunc;
    result.push_back(std::move(func));
    return result;
}

} // namespace json_extension
} // namespace kuzu
