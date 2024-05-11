#pragma once

#include "common/types/uuid.h"
#include "function/function.h"
#include "main/client_context.h"

namespace kuzu {
namespace function {

struct GenRandomUUID {
    static void operation(common::ku_uuid_t& input, void* dataPtr) {
        input = common::UUID::generateRandomUUID(
            reinterpret_cast<FunctionBindData*>(dataPtr)->clientContext->getRandomEngine());
    }
};

} // namespace function
} // namespace kuzu
