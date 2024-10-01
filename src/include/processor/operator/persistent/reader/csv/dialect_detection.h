#pragma once

#include <vector>

#include "common/copier_config/csv_reader_config.h"

using namespace kuzu::common;

struct DialectOption {
    char delimiter = ',';
    char quoteChar = '"';
    char escapeChar = '"';
    bool everQuoted = false;
};

struct DialectCandidates {
    static constexpr std::array delimiters = CopyConstants::DEFAULT_CSV_DELIMITER_SEARCH_SPACE;
    static constexpr std::array quoteChars = CopyConstants::DEFAULT_CSV_QUOTE_SEARCH_SPACE;
    static constexpr std::array escapeChars = CopyConstants::DEFAULT_CSV_ESCAPE_SEARCH_SPACE;
};

std::vector<DialectOption> generateDialectOptions(const DialectCandidates& candidates,
    const CSVOption& option);
