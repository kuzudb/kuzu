#pragma once

#include "common/types/value/value.h"
#include "main/client_context.h"

namespace kuzu {
namespace main {

struct ThreadsSetting {
    static constexpr const char* name = "threads";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::INT64);
        context->numThreadsForExecution = parameter.getValue<int64_t>();
    }
    static std::string getSetting(ClientContext* context) {
        return std::to_string(context->numThreadsForExecution);
    }
};

struct TimeoutSetting {
    static constexpr const char* name = "timeout";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::INT64);
        context->timeoutInMS = parameter.getValue<int64_t>();
        context->startTimingIfEnabled();
    }
    static std::string getSetting(ClientContext* context) {
        return std::to_string(context->timeoutInMS);
    }
};

struct VarLengthExtendMaxDepthSetting {
    static constexpr const char* name = "var_length_extend_max_depth";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::INT64);
        context->varLengthExtendMaxDepth = parameter.getValue<int64_t>();
    }
    static std::string getSetting(ClientContext* context) {
        return std::to_string(context->varLengthExtendMaxDepth);
    }
};

struct EnableSemiMaskSetting {
    static constexpr const char* name = "enable_semi_mask";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::BOOL);
        context->enableSemiMask = parameter.getValue<bool>();
    }
    static std::string getSetting(ClientContext* context) {
        return context->enableSemiMask ? "true" : "false";
    }
};

} // namespace main
} // namespace kuzu
