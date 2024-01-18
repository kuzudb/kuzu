#pragma once

#include "common/types/uuid.h"
#include "main/client_context.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace common {
struct uuid_t;
} // namespace common

namespace function {

struct GenRandomUUID {
    static inline void operation(common::ValueVector& result, void* dataPtr) {
        KU_ASSERT(result.state->isFlat());
        auto resultValues = (common::uuid_t*)result.getData();
        auto idx = result.state->selVector->selectedPositions[0];
        KU_ASSERT(idx == 0);
        resultValues[idx] = common::uuid_t::generateRandomUUID(
            reinterpret_cast<main::ClientContext*>(dataPtr)->getRandomEngine());
    }
};

} // namespace function
} // namespace kuzu
