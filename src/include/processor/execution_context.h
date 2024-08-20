#pragma once

#include "common/profiler.h"
#include "main/client_context.h"

namespace kuzu {
namespace processor {

class FactorizedTable;
struct PopulatedCSVError;

struct ExecutionContext {
    uint64_t queryID;
    uint64_t warningCount;
    common::Profiler* profiler;
    main::ClientContext* clientContext;

    ExecutionContext(common::Profiler* profiler, main::ClientContext* clientContext,
        uint64_t queryID)
        : queryID{queryID}, warningCount(0), profiler{profiler}, clientContext{clientContext} {}

    void appendWarningMessages(const std::vector<PopulatedCSVError>& messages, uint64_t queryID) {
        warningCount += messages.size();
        clientContext->getWarningContextUnsafe().appendWarningMessages(messages, queryID);
    }
};

} // namespace processor
} // namespace kuzu
