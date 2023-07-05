#pragma once

#include "common/types/types.h"
#include "main/client_context.h"

namespace kuzu {
namespace main {

typedef void (*set_context)(ClientContext* context, const common::Value& parameter);
typedef std::string (*get_setting)(ClientContext* context);

struct ConfigurationOption {
    const char* name;
    common::LogicalTypeID parameterType;
    set_context setContext;
    get_setting getSetting;
};

class DBConfig {
public:
    static ConfigurationOption* getOptionByName(const std::string& optionName);
};

} // namespace main
} // namespace kuzu
