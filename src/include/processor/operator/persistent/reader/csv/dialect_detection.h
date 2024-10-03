#pragma once

#include <vector>

#include "common/copier_config/csv_reader_config.h"

struct DialectOption {
    char delimiter = ',';
    char quoteChar = '"';
    char escapeChar = '"';
    bool everQuoted = false;
    bool doDialectDetection = true;

    DialectOption() = default;
    DialectOption(char delim, char quote, char escape)
        : delimiter(delim), quoteChar(quote), escapeChar(escape), everQuoted(false), doDialectDetection(true) {}
};

std::vector<DialectOption> generateDialectOptions(const kuzu::common::CSVOption& option);
