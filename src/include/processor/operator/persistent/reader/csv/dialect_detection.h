#pragma once

#include <vector>

#include "common/copier_config/csv_reader_config.h"

using namespace kuzu::common;

struct DialectOption {
    char delimiter = ',';
    char quoteChar = '"';
    char escapeChar = '"';
    bool everQuoted = false;

    DialectOption() = default;
    DialectOption(char delim, char quote, char escape)
        : delimiter(delim), quoteChar(quote), escapeChar(escape), everQuoted(false) {}
};

std::vector<DialectOption> generateDialectOptions(const CSVOption& option);
