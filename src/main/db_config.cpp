#include "main/db_config.h"

#include "common/string_utils.h"
#include "main/database.h"
#include "main/settings.h"

using namespace kuzu::common;

namespace kuzu {
namespace main {

#define GET_CONFIGURATION(_PARAM)                                                                  \
    { _PARAM::name, _PARAM::inputType, _PARAM::setContext, _PARAM::getSetting }

static ConfigurationOption options[] = { // NOLINT(cert-err58-cpp):
    GET_CONFIGURATION(ThreadsSetting), GET_CONFIGURATION(TimeoutSetting),
    GET_CONFIGURATION(WarningLimitSetting), GET_CONFIGURATION(VarLengthExtendMaxDepthSetting),
    GET_CONFIGURATION(EnableSemiMaskSetting), GET_CONFIGURATION(DisableMapKeyCheck),
    GET_CONFIGURATION(EnableZoneMapSetting), GET_CONFIGURATION(HomeDirectorySetting),
    GET_CONFIGURATION(FileSearchPathSetting), GET_CONFIGURATION(ProgressBarSetting),
    GET_CONFIGURATION(RecursivePatternSemanticSetting),
    GET_CONFIGURATION(RecursivePatternFactorSetting), GET_CONFIGURATION(EnableMVCCSetting),
    GET_CONFIGURATION(CheckpointThresholdSetting), GET_CONFIGURATION(AutoCheckpointSetting),
    GET_CONFIGURATION(ForceCheckpointClosingDBSetting), GET_CONFIGURATION(SpillToDiskSetting),
    GET_CONFIGURATION(EnableGDSSetting)};

DBConfig::DBConfig(const SystemConfig& systemConfig)
    : bufferPoolSize{systemConfig.bufferPoolSize}, maxNumThreads{systemConfig.maxNumThreads},
      enableCompression{systemConfig.enableCompression}, readOnly{systemConfig.readOnly},
      maxDBSize{systemConfig.maxDBSize}, enableMultiWrites{false},
      autoCheckpoint{systemConfig.autoCheckpoint},
      checkpointThreshold{systemConfig.checkpointThreshold}, forceCheckpointOnClose{true},
      enableSpillingToDisk{true} {}

ConfigurationOption* DBConfig::getOptionByName(const std::string& optionName) {
    auto lOptionName = optionName;
    StringUtils::toLower(lOptionName);
    for (auto& internalOption : options) {
        if (internalOption.name == lOptionName) {
            return &internalOption;
        }
    }
    return nullptr;
}

bool DBConfig::isDBPathInMemory(const std::string& dbPath) {
    return dbPath.empty() || dbPath == ":memory:";
}

} // namespace main
} // namespace kuzu
