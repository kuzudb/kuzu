#pragma once

#include <string>

#include "common/types/value/value.h"

namespace kuzu {
namespace common {
class Value;
enum class LogicalTypeID : uint8_t;
} // namespace common

namespace main {

class ClientContext;
struct SystemConfig;

typedef void (*set_context)(ClientContext* context, const common::Value& parameter);
typedef common::Value (*get_setting)(ClientContext* context);

enum class OptionType : uint8_t { CONFIGURATION = 0, EXTENSION = 1 };

struct Option {
    std::string name;
    common::LogicalTypeID parameterType;
    OptionType optionType;

    Option(std::string name, common::LogicalTypeID parameterType, OptionType optionType)
        : name{std::move(name)}, parameterType{std::move(parameterType)}, optionType{optionType} {}

    virtual ~Option() = default;
};

struct ConfigurationOption : public Option {
    set_context setContext;
    get_setting getSetting;

    ConfigurationOption(std::string name, common::LogicalTypeID parameterType,
        set_context setContext, get_setting getSetting)
        : Option{std::move(name), parameterType, OptionType::CONFIGURATION}, setContext{setContext},
          getSetting{getSetting} {}
};

struct ExtensionOption : public Option {
    common::Value defaultValue;

    ExtensionOption(std::string name, common::LogicalTypeID parameterType,
        common::Value defaultValue)
        : Option{std::move(name), parameterType, OptionType::EXTENSION},
          defaultValue{std::move(defaultValue)} {}
};

class DBConfig {
public:
    static ConfigurationOption* getOptionByName(const std::string& optionName);

    explicit DBConfig(SystemConfig& systemConfig);

    uint64_t bufferPoolSize;
    uint64_t maxNumThreads;
    bool enableCompression;
    bool readOnly;
    uint64_t maxDBSize;
    bool enableMultiWrites = false;
};

} // namespace main
} // namespace kuzu
