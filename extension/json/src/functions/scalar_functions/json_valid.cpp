#include "function/scalar_function.h"
#include "json_scalar_functions.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void execFunc(std::span<const common::SelectedVector> parameters,
    common::SelectedVector result, void* /*dataPtr*/) {
    for (auto selectedPos = 0u; selectedPos < result.sel->getSelSize(); ++selectedPos) {
        auto inputPos = (*parameters[0].sel)[selectedPos];
        auto resultPos = (*result.sel)[selectedPos];
        auto isNull = parameters[0].vec.isNull(inputPos);
        result.vec.setNull(resultPos, isNull);
        if (!isNull) {
            result.vec.setValue<bool>(resultPos,
                stringToJsonNoError(parameters[0].vec.getValue<ku_string_t>(inputPos).getAsString())
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
