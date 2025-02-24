#include "function/scalar_function.h"
#include "json_scalar_functions.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {
        auto inputPos = (*parameterSelVectors[0])[selectedPos];
        auto resultPos = (*resultSelVector)[selectedPos];
        auto isNull = parameters[0]->isNull(inputPos);
        result.setNull(resultPos, isNull);
        if (!isNull) {
            result.setValue<bool>(resultPos,
                stringToJsonNoError(parameters[0]->getValue<ku_string_t>(inputPos).getAsString())
                        .ptr != nullptr);
        }
    }
}

function_set JsonValidFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::BOOL, execFunc));
    return result;
}

} // namespace json_extension
} // namespace kuzu
