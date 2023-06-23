#pragma once

#include "common/types/types.h"
#include "main/client_context.h"

namespace kuzu {
namespace main {

typedef void (*set_context)(ClientContext* context, const common::Value& parameter);

struct ConfigurationOption {
    const char* name;
    common::LogicalTypeID parameterType;
    set_context setContext;
};

class DBConfig {
public:
    static ConfigurationOption* getOptionByName(const std::string& optionName);
};

} // namespace main
} // namespace kuzu
