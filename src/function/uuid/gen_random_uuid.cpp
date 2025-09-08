#include "function/uuid/functions/gen_random_uuid.h"

#include "function/function.h"
#include "main/client_context.h"

namespace kuzu {
namespace function {

void GenRandomUUID::operation(common::ku_uuid_t& input, void* dataPtr) {
    input = common::UUID::generateRandomUUID(
        static_cast<FunctionBindData*>(dataPtr)->clientContext->getRandomEngine());
}

} // namespace function
} // namespace kuzu
