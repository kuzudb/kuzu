#include "function/uuid/vector_uuid_functions.h"

#include "function/uuid/functions/gen_random_uuid.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

function_set GenRandomUUIDFunction::getFunctionSet() {
    function_set definitions;
    definitions.push_back(
        make_unique<ScalarFunction>(GEN_RANDOM_UUID_FUNC_NAME, std::vector<LogicalTypeID>{},
            LogicalTypeID::UUID, ScalarFunction::PoniterExecFunction<ku_uuid_t, GenRandomUUID>));
    return definitions;
}

} // namespace function
} // namespace kuzu
