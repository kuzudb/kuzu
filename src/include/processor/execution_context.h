#pragma once

#include "common/profiler.h"
#include "main/client_context.h"

namespace kuzu {
namespace processor {

struct ExecutionContext {
    common::Profiler* profiler;
    main::ClientContext* clientContext;

    ExecutionContext(common::Profiler* profiler, main::ClientContext* clientContext)
        : profiler{profiler}, clientContext{clientContext} {}
};

} // namespace processor
} // namespace kuzu
