#include "main/db_config.h"

#include "common/string_utils.h"
#include "main/settings.h"

using namespace kuzu::common;

namespace kuzu {
namespace main {

#define GET_CONFIGURATION(_PARAM)                                                                  \
    { _PARAM::name, _PARAM::inputType, _PARAM::setContext, _PARAM::getSetting }

static ConfigurationOption options[] = {GET_CONFIGURATION(ThreadsSetting),
    GET_CONFIGURATION(TimeoutSetting), GET_CONFIGURATION(VarLengthExtendMaxDepthSetting),
    GET_CONFIGURATION(EnableSemiMaskSetting)};

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

} // namespace main
} // namespace kuzu
