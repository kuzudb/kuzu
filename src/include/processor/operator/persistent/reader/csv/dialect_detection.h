#pragma once

#include <vector>

#include "common/copier_config/csv_reader_config.h"

using namespace kuzu::common;

struct DialectOption {
    char delimiter = ',';
    char quoteChar = '"';
    char escapeChar = '"';
    bool ever_quoted = false;
};

struct DialectCandidates {
    std::vector<char> delimiters;
    std::vector<char> quoteChars;
    std::vector<char> escapeChars;

    // Constructor to initialize the default candidates
    DialectCandidates();
};

// Function to generate all combinations of dialect options
std::vector<DialectOption> generateDialectOptions(const DialectCandidates& candidates,
    const CSVOption& option);
