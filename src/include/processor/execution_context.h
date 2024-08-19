#pragma once

#include "common/profiler.h"
#include "main/client_context.h"
#include "processor/warning_context.h"

namespace kuzu {
namespace processor {

class FactorizedTable;
struct PopulatedCSVError;

struct ExecutionContext {
    uint64_t queryID;
    common::Profiler* profiler;
    main::ClientContext* clientContext;
    processor::WarningContext warningContext;

    ExecutionContext(common::Profiler* profiler, main::ClientContext* clientContext,
        uint64_t queryID)
        : queryID{queryID}, profiler{profiler}, clientContext{clientContext},
          warningContext(this->clientContext) {}
};

} // namespace processor
} // namespace kuzu
