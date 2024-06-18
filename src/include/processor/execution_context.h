#pragma once

#include "common/profiler.h"
#include "common/types/uuid.h"
#include "main/client_context.h"

namespace kuzu {
namespace processor {

struct ExecutionContext {
    std::string queryId;
    common::Profiler* profiler;
    main::ClientContext* clientContext;

    ExecutionContext(common::Profiler* profiler, main::ClientContext* clientContext, std::string id)
        : profiler{profiler}, clientContext{clientContext} {
        if (id.empty()) {
            common::UUID::toString(
                common::UUID::generateRandomUUID(clientContext->getRandomEngine()));
        } else {
            queryId = id;
        }
    }
};

} // namespace processor
} // namespace kuzu
