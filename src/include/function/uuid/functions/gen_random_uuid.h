#pragma once

#include "common/types/uuid.h"

namespace kuzu {
namespace function {

struct GenRandomUUID {
    static void operation(common::ku_uuid_t& input, void* dataPtr);
};

} // namespace function
} // namespace kuzu
