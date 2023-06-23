#pragma once

#include "common/types/value.h"
#include "main/client_context.h"

namespace kuzu {
namespace main {

struct ThreadsSetting {
    static constexpr const char* name = "threads";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        assert(parameter.getDataType().getLogicalTypeID() == common::LogicalTypeID::INT64);
        context->numThreadsForExecution = parameter.getValue<int64_t>();
    }
};

struct TimeoutSetting {
    static constexpr const char* name = "timeout";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        assert(parameter.getDataType().getLogicalTypeID() == common::LogicalTypeID::INT64);
        context->timeoutInMS = parameter.getValue<int64_t>();
        context->startTimingIfEnabled();
    }
};

} // namespace main
} // namespace kuzu
