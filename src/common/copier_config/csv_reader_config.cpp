#include "common/copier_config/csv_reader_config.h"

#include <algorithm>

#include "common/exception/binder.h"

namespace kuzu {
namespace common {

static char bindParsingOptionValue(std::string value) {
    if (value == "\\t") {
        return '\t';
    }
    if (value.length() < 1 || value.length() > 2 || (value.length() == 2 && value[0] != '\\')) {
        throw BinderException("Copy csv option value must be a single character with an "
                              "optional escape character.");
    }
    return value[value.length() - 1];
}

static void bindBoolParsingOption(CSVReaderConfig& config, const std::string& optionName,
    bool optionValue) {
    if (optionName == "HEADER") {
        config.option.hasHeader = optionValue;
    } else if (optionName == "PARALLEL") {
        config.parallel = optionValue;
    }
}

static void bindStringParsingOption(CSVReaderConfig& config, const std::string& optionName,
    const std::string& optionValue) {
    auto parsingOptionValue = bindParsingOptionValue(optionValue);
    if (optionName == "ESCAPE") {
        config.option.escapeChar = parsingOptionValue;
    } else if (optionName == "DELIM") {
        config.option.delimiter = parsingOptionValue;
    } else if (optionName == "QUOTE") {
        config.option.quoteChar = parsingOptionValue;
    }
}

template<uint64_t size>
static bool hasOption(const char* const (&arr)[size], const std::string& option) {
    return std::find(std::begin(arr), std::end(arr), option) != std::end(arr);
}

static bool validateBoolParsingOptionName(const std::string& parsingOptionName) {
    return hasOption(CopyConstants::BOOL_CSV_PARSING_OPTIONS, parsingOptionName);
}

static bool validateStringParsingOptionName(const std::string& parsingOptionName) {
    return hasOption(CopyConstants::STRING_CSV_PARSING_OPTIONS, parsingOptionName);
}

CSVReaderConfig CSVReaderConfig::construct(
    const std::unordered_map<std::string, common::Value>& options) {
    auto config = CSVReaderConfig();
    for (auto& op : options) {
        auto name = op.first;
        auto isValidStringParsingOption = validateStringParsingOptionName(name);
        auto isValidBoolParsingOption = validateBoolParsingOptionName(name);
        if (isValidBoolParsingOption) {
            if (*op.second.getDataType() != *LogicalType::BOOL()) {
                throw BinderException(
                    stringFormat("The type of csv parsing option {} must be a boolean.", name));
            }
            bindBoolParsingOption(config, name, op.second.getValue<bool>());
        } else if (isValidStringParsingOption) {
            if (*op.second.getDataType() != *LogicalType::STRING()) {
                throw BinderException(
                    stringFormat("The type of csv parsing option {} must be a string.", name));
            }
            bindStringParsingOption(config, name, op.second.getValue<std::string>());
        } else {
            throw BinderException(stringFormat("Unrecognized csv parsing option: {}.", name));
        }
    }
    return config;
}

} // namespace common
} // namespace kuzu
