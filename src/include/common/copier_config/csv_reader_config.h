#pragma once

#include "common/constants.h"
#include "common/copy_constructors.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace common {

struct CSVOption {
    // TODO(Xiyang): Add newline character option and delimiter can be a string.
    char escapeChar;
    char delimiter;
    char quoteChar;
    bool hasHeader;

    CSVOption()
        : escapeChar{CopyConstants::DEFAULT_CSV_ESCAPE_CHAR},
          delimiter{CopyConstants::DEFAULT_CSV_DELIMITER},
          quoteChar{CopyConstants::DEFAULT_CSV_QUOTE_CHAR},
          hasHeader{CopyConstants::DEFAULT_CSV_HAS_HEADER} {}
    EXPLICIT_COPY_DEFAULT_MOVE(CSVOption);

    // TODO: COPY FROM and COPY TO should support transform special options, like '\'.
    std::string toCypher() const {
        std::string header = hasHeader ? "true" : "false";
        return stringFormat("(escape ='\\{}', delim ='{}', quote='\\{}', header={})", escapeChar,
            delimiter, quoteChar, header);
    }

private:
    CSVOption(const CSVOption& other)
        : escapeChar{other.escapeChar}, delimiter{other.delimiter}, quoteChar{other.quoteChar},
          hasHeader{other.hasHeader} {}
};

struct CSVReaderConfig {
    CSVOption option;
    bool parallel;

    CSVReaderConfig() : option{}, parallel{CopyConstants::DEFAULT_CSV_PARALLEL} {}
    EXPLICIT_COPY_DEFAULT_MOVE(CSVReaderConfig);

    static CSVReaderConfig construct(const std::unordered_map<std::string, common::Value>& options);

private:
    CSVReaderConfig(const CSVReaderConfig& other)
        : option{other.option.copy()}, parallel{other.parallel} {}
};

} // namespace common
} // namespace kuzu
