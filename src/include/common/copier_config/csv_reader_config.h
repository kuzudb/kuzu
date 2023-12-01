#pragma once

#include <memory>

#include "common/constants.h"
#include "reader_config.h"

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
    CSVOption(const CSVOption& other)
        : escapeChar{other.escapeChar}, delimiter{other.delimiter}, quoteChar{other.quoteChar},
          hasHeader{other.hasHeader} {}

    virtual ~CSVOption() = default;

    inline std::unique_ptr<CSVOption> copy() const { return std::make_unique<CSVOption>(*this); }
};

struct CSVReaderConfig final : public ExtraReaderConfig {
    CSVOption option;
    bool parallel;

    CSVReaderConfig() : option{}, parallel{CopyConstants::DEFAULT_CSV_PARALLEL} {}
    CSVReaderConfig(const CSVReaderConfig& other)
        : option{other.option}, parallel{other.parallel} {}

    inline std::unique_ptr<ExtraReaderConfig> copy() override {
        return std::make_unique<CSVReaderConfig>(*this);
    }
};

} // namespace common
} // namespace kuzu
