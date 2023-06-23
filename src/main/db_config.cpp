#include "main/db_config.h"

#include "common/string_utils.h"
#include "main/settings.h"

namespace kuzu {
namespace main {

#define GET_CONFIGURATION(_PARAM)                                                                  \
    { _PARAM::name, _PARAM::inputType, _PARAM::setContext, }

static ConfigurationOption options[] = {
    GET_CONFIGURATION(ThreadsSetting), GET_CONFIGURATION(TimeoutSetting)};

ConfigurationOption* DBConfig::getOptionByName(const std::string& optionName) {
    auto lOptionName = optionName;
    common::StringUtils::toLower(lOptionName);
    for (auto& internalOption : options) {
        if (internalOption.name == lOptionName) {
            return &internalOption;
        }
    }
    return nullptr;
}

} // namespace main
} // namespace kuzu
