#include "function/scalar_function.h"
#include "json_scalar_functions.h"
#include "json_utils.h"

namespace kuzu {
namespace json_extension {

using namespace function;
using namespace common;

static void jsonArrayLength(std::span<const common::SelectedVector> parameters,
    common::SelectedVector result, void* /*dataPtr*/) {
    KU_ASSERT(parameters.size() == 1);
    for (auto i = 0u; i < result.sel->getSelSize(); ++i) {
        auto inputPos = (*parameters[0].sel)[i];
        auto resultPos = (*result.sel)[i];
        auto isNull = parameters[0].vec.isNull(inputPos);
        result.vec.setNull(resultPos, isNull);
        if (!isNull) {
            result.vec.setValue<uint32_t>(resultPos,
                jsonArraySize(
                    stringToJson(parameters[0].vec.getValue<ku_string_t>(inputPos).getAsString())));
        }
    }
}

function_set JsonArrayLengthFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::UINT32, jsonArrayLength));
    return result;
}

} // namespace json_extension
} // namespace kuzu
