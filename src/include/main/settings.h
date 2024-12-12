#pragma once

#include "common/task_system/progress_bar.h"
#include "common/types/value/value.h"
#include "main/client_context.h"
#include "main/db_config.h"

namespace kuzu {
namespace main {

struct ThreadsSetting {
    static constexpr auto name = "threads";
    static constexpr auto inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->numThreads = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getClientConfig()->numThreads);
    }
};

struct WarningLimitSetting {
    static constexpr auto name = "warning_limit";
    static constexpr auto inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->warningLimit = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getClientConfig()->warningLimit);
    }
};

struct TimeoutSetting {
    static constexpr auto name = "timeout";
    static constexpr auto inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->timeoutInMS = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getClientConfig()->timeoutInMS);
    }
};

struct ProgressBarSetting {
    static constexpr auto name = "progress_bar";
    static constexpr auto inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->enableProgressBar = parameter.getValue<bool>();
        context->getProgressBar()->toggleProgressBarPrinting(parameter.getValue<bool>());
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getClientConfig()->enableProgressBar);
    }
};

struct VarLengthExtendMaxDepthSetting {
    static constexpr auto name = "var_length_extend_max_depth";
    static constexpr auto inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->varLengthMaxDepth = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getClientConfig()->varLengthMaxDepth);
    }
};

struct EnableSemiMaskSetting {
    static constexpr auto name = "enable_semi_mask";
    static constexpr auto inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->enableSemiMask = parameter.getValue<bool>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getClientConfig()->enableSemiMask);
    }
};

struct DisableMapKeyCheck {
    static constexpr auto name = "disable_map_key_check";
    static constexpr auto inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->disableMapKeyCheck = parameter.getValue<bool>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getClientConfig()->disableMapKeyCheck);
    }
};

struct EnableZoneMapSetting {
    static constexpr auto name = "enable_zone_map";
    static constexpr auto inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->enableZoneMap = parameter.getValue<bool>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getClientConfig()->enableZoneMap);
    }
};

struct EnableGDSSetting {
    static constexpr auto name = "enable_gds";
    static constexpr auto inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->enableGDS = parameter.getValue<bool>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getClientConfig()->enableGDS);
    }
};

struct HomeDirectorySetting {
    static constexpr auto name = "home_directory";
    static constexpr auto inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->homeDirectory = parameter.getValue<std::string>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value::createValue(context->getClientConfig()->homeDirectory);
    }
};

struct FileSearchPathSetting {
    static constexpr auto name = "file_search_path";
    static constexpr auto inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->fileSearchPath = parameter.getValue<std::string>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value::createValue(context->getClientConfig()->fileSearchPath);
    }
};

struct RecursivePatternSemanticSetting {
    static constexpr auto name = "recursive_pattern_semantic";
    static constexpr auto inputType = common::LogicalTypeID::STRING;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        const auto input = parameter.getValue<std::string>();
        context->getClientConfigUnsafe()->recursivePatternSemantic =
            common::PathSemanticUtils::fromString(input);
    }
    static common::Value getSetting(const ClientContext* context) {
        const auto result = common::PathSemanticUtils::toString(
            context->getClientConfig()->recursivePatternSemantic);
        return common::Value::createValue(result);
    }
};

struct RecursivePatternFactorSetting {
    static constexpr auto name = "recursive_pattern_factor";
    static constexpr auto inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->recursivePatternCardinalityScaleFactor =
            parameter.getValue<std::int64_t>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value::createValue(
            context->getClientConfig()->recursivePatternCardinalityScaleFactor);
    }
};

struct EnableMVCCSetting {
    static constexpr auto name = "debug_enable_multi_writes";
    static constexpr auto inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        KU_ASSERT(parameter.getDataType().getLogicalTypeID() == common::LogicalTypeID::BOOL);
        // TODO: This is a temporary solution to make tests of multiple write transactions easier.
        context->getDBConfigUnsafe()->enableMultiWrites = parameter.getValue<bool>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getDBConfig()->enableMultiWrites);
    }
};

struct CheckpointThresholdSetting {
    static constexpr auto name = "checkpoint_threshold";
    static constexpr auto inputType = common::LogicalTypeID::INT64;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getDBConfigUnsafe()->checkpointThreshold = parameter.getValue<int64_t>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getDBConfig()->checkpointThreshold);
    }
};

struct AutoCheckpointSetting {
    static constexpr auto name = "auto_checkpoint";
    static constexpr auto inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getDBConfigUnsafe()->autoCheckpoint = parameter.getValue<bool>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getDBConfig()->autoCheckpoint);
    }
};

struct ForceCheckpointClosingDBSetting {
    static constexpr auto name = "force_checkpoint_on_close";
    static constexpr auto inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getDBConfigUnsafe()->forceCheckpointOnClose = parameter.getValue<bool>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value(context->getDBConfig()->forceCheckpointOnClose);
    }
};

struct SpillToDiskSetting {
    static constexpr auto name = "spill_to_disk";
    static constexpr auto inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter);
    static common::Value getSetting(const ClientContext* context) {
        return common::Value::createValue(context->getDBConfig()->enableSpillingToDisk);
    }
};

struct EnableOptimizerSetting {
    static constexpr auto name = "enable_plan_optimizer";
    static constexpr auto inputType = common::LogicalTypeID::BOOL;
    static void setContext(ClientContext* context, const common::Value& parameter) {
        parameter.validateType(inputType);
        context->getClientConfigUnsafe()->enablePlanOptimizer = parameter.getValue<bool>();
    }
    static common::Value getSetting(const ClientContext* context) {
        return common::Value::createValue(context->getClientConfig()->enablePlanOptimizer);
    }
};

} // namespace main
} // namespace kuzu
