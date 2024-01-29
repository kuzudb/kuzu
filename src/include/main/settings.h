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
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->numThreadsForExecution);
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
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->timeoutInMS);
    }
};

struct VarLengthExtendMaxDepthSetting {
    static constexpr const char* name = "var_length_extend_max_depth";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::INT64);
        context->varLengthExtendMaxDepth = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->varLengthExtendMaxDepth);
    }
};

struct EnableSemiMaskSetting {
    static constexpr const char* name = "enable_semi_mask";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::BOOL);
        context->enableSemiMask = parameter.getValue<bool>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->enableSemiMask);
    }
};

struct HomeDirectorySetting {
    static constexpr const char* name = "home_directory";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::STRING);
        context->homeDirectory = parameter.getValue<std::string>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value::createValue(context->homeDirectory);
    }
};

struct FileSearchPathSetting {
    static constexpr const char* name = "file_search_path";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::STRING);
        context->fileSearchPath = parameter.getValue<std::string>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value::createValue(context->fileSearchPath);
    }
};

} // namespace main
} // namespace kuzu
