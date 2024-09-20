#pragma once

#include "common/copier_config/csv_reader_config.h"

#include <string>
#include <vector>

using namespace kuzu::common;

struct DialectOption {
    char delimiter = ',';
    char quoteChar = '"';
    char escapeChar = '"';
};

struct DialectCandidates {
    std::vector<char> delimiters;
    std::vector<char> quoteChars;
    std::vector<char> escapeChars;

    // Constructor to initialize the default candidates
    DialectCandidates();
};

// Function to generate all combinations of dialect options
std::vector<DialectOption> generateDialectOptions(const DialectCandidates& candidates);

// Function to parse a sample line using the specified dialect
bool parseCSVLine(const std::string& line, const DialectOption& option, std::vector<std::string>& fields);

// Function to parse a sample CSV data and collect the field counts for each row
bool parseSampleWithDialect(const std::string& sample, const DialectOption& option, std::vector<int>& fieldCounts);

// Function to score the consistency of a dialect based on the field counts across rows
int scoreDialect(const std::vector<int>& fieldCounts);

// Function to analyze the sample for the best dialect option
void analyzeSampleForDialect(const std::string& sample, DialectOption& dialect, CSVOption& option);
