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
        context->getClientConfigUnsafe()->numThreads = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->numThreads);
    }
};

struct TimeoutSetting {
    static constexpr const char* name = "timeout";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::INT64);
        context->getClientConfigUnsafe()->timeoutInMS = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->timeoutInMS);
    }
};

struct ProgressBarSetting {
    static constexpr const char* name = "progress_bar";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::BOOL);
        context->getClientConfigUnsafe()->enableProgressBar = parameter.getValue<bool>();
        context->getProgressBar()->toggleProgressBarPrinting(parameter.getValue<bool>());
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->enableProgressBar);
    }
};

struct ProgressBarTimerSetting {
    static constexpr const char* name = "progress_bar_time";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::INT64);
        context->getClientConfigUnsafe()->showProgressAfter = parameter.getValue<int64_t>();
        context->getProgressBar()->setShowProgressAfter(parameter.getValue<int64_t>());
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->showProgressAfter);
    }
};

struct VarLengthExtendMaxDepthSetting {
    static constexpr const char* name = "var_length_extend_max_depth";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::INT64);
        context->getClientConfigUnsafe()->varLengthMaxDepth = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->varLengthMaxDepth);
    }
};

struct EnableSemiMaskSetting {
    static constexpr const char* name = "enable_semi_mask";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::BOOL);
        context->getClientConfigUnsafe()->enableSemiMask = parameter.getValue<bool>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->enableSemiMask);
    }
};

struct HomeDirectorySetting {
    static constexpr const char* name = "home_directory";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::STRING);
        context->getClientConfigUnsafe()->homeDirectory = parameter.getValue<std::string>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value::createValue(context->getClientConfig()->homeDirectory);
    }
};

struct FileSearchPathSetting {
    static constexpr const char* name = "file_search_path";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::STRING);
        context->getClientConfigUnsafe()->fileSearchPath = parameter.getValue<std::string>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value::createValue(context->getClientConfig()->fileSearchPath);
    }
};

struct EnableMultiCopySetting {
    static constexpr const char* name = "enable_multi_copy";
    static constexpr const common::LogicalTypeID inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType()->getLogicalTypeID() == common::LogicalTypeID::BOOL);
        context->getClientConfigUnsafe()->enableMultiCopy = parameter.getValue<bool>();
    }
    static common::Value getSetting(ClientContext* context) {
        return common::Value(context->getClientConfig()->enableMultiCopy);
    }
};

} // namespace main
} // namespace kuzu
