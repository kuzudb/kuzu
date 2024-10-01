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
    std::vector<char> delimiters;
    std::vector<char> quoteChars;
    std::vector<char> escapeChars;

    DialectCandidates();
};

std::vector<DialectOption> generateDialectOptions(const DialectCandidates& candidates,
    const CSVOption& option);
