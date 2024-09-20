#include "processor/operator/persistent/reader/csv/dialect_detection.h"

#include <iostream>
#include <sstream>
#include <unordered_map>
#include <algorithm>

using namespace kuzu::common;

// Constructor for DialectCandidates to initialize default values
DialectCandidates::DialectCandidates() {
    delimiters = {',', ';', '\t', '|'};
    quoteChars = {'"', '\''};
    escapeChars = {'"', '\\', '\''};
}

// Generates all combinations of dialect options
std::vector<DialectOption> generateDialectOptions(const DialectCandidates& candidates) {
    std::vector<DialectOption> options;

    for (char delim : candidates.delimiters) {
        for (char quote : candidates.quoteChars) {
            for (char escape : candidates.escapeChars) {
                DialectOption option{delim, quote, escape};
                options.push_back(option);
            }
        }
    }
    return options;
}

// Parses a single CSV line using the given dialect and fills the fields
bool parseCSVLine(const std::string& line, const DialectOption& option, std::vector<std::string>& fields) {
    fields.clear();
    std::string field;
    bool inQuotes = false;

    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];

        if (inQuotes) {
            //TODO: BUG here if escapeChar
            if (c == option.escapeChar && c == option.quoteChar && i + 1 < line.size() && c == line[i+1]) {
                // Handle escaped character
                field += line[i + 1];
                ++i; // Skip the escaped character
            } else if (c == option.quoteChar) {
                // End of quoted field
                inQuotes = false;
            } else if (c == option.escapeChar && i + 1 < line.size()) {
                // Handle escaped character
                field += line[i + 1];
                ++i; // Skip the escaped character
            }  else {
                field += c;
            }
        } else {
            if (c == option.quoteChar) {
                // Start of quoted field
                inQuotes = true;
            } else if (c == option.delimiter) {
                // End of field
                fields.push_back(field);
                field.clear();
            } else {
                field += c;
            }
        }
    }

    // Add the last field
    fields.push_back(field);

    // Ensure no unclosed quotes
    return !inQuotes;
}

// Parses a sample CSV using the given dialect and collects the field counts per row
bool parseSampleWithDialect(const std::string& sample, const DialectOption& option, std::vector<int>& fieldCounts) {
    std::vector<std::string> lines;
    std::istringstream stream(sample);
    std::string line;

    bool success = true;

    while (std::getline(stream, line)) {
        if (line.empty()) {
            continue; // Skip empty lines
        }

        std::vector<std::string> fields;
        if (!parseCSVLine(line, option, fields)) {
            success = false;
            fieldCounts.push_back(-1);
            continue; // Failed to parse the line, skip this dialect
        }

        // ONLY when delimiter is actually being found
        if (fields.size() > 1) {
            fieldCounts.push_back(fields.size());
        } else {
            success = false;
            break; // This delimiter is not valid since it didn't split the line into multiple fields
        }
    }

    return success;
}

// Scores the consistency of a dialect based on the most frequent number of fields per row
int scoreDialect(const std::vector<int>& fieldCounts) {
    if (fieldCounts.empty()) {
        return 0;
    }

    // Map to count occurrences of each field count
    std::unordered_map<int, int> fieldCountFrequency;
    for (int count : fieldCounts) {
        fieldCountFrequency[count]++;
    }

    // Find the most frequent field count (mode)
    int mostFrequentFieldCount = 0;
    int highestFrequency = 0;
    for (const auto& entry : fieldCountFrequency) {
        if (entry.second > highestFrequency) {
            mostFrequentFieldCount = entry.first;
            highestFrequency = entry.second;
        }
    }

    // Calculate the number of rows that match the most frequent field count
    int consistentRows = std::count(fieldCounts.begin(), fieldCounts.end(), mostFrequentFieldCount);

    return consistentRows;
}


void analyzeSampleForDialect(const std::string& sample, DialectOption& dialect, CSVOption& option) {
    DialectCandidates candidates;

    dialect.delimiter = option.delimiter;  // Default to the first option in delimiters
    dialect.quoteChar = option.quoteChar;  // Default to the first option in quoteChars
    dialect.escapeChar = option.escapeChar;  // Default to the first option in escapeChars


    // Generate dialect options based on the non-user-specified options
    auto dialectOptions = generateDialectOptions(candidates);

    DialectOption bestDialect = dialect;  // Start with the user-provided values as the base
    int bestScore = 0;

    for (const auto& dialectOption : dialectOptions) {
        // Skip if the user has already set the options, don't overwrite them
        DialectOption tempOption = dialect;  // Copy user-provided values
        if (!option.setDelim) {
            tempOption.delimiter = dialectOption.delimiter;
        }
        if (!option.setQuote) {
            tempOption.quoteChar = dialectOption.quoteChar;
        }
        if (!option.setEscape) {
            tempOption.escapeChar = dialectOption.escapeChar;
        }

        std::vector<int> fieldCounts;
        if (parseSampleWithDialect(sample, tempOption, fieldCounts)) {
            int score = scoreDialect(fieldCounts);
            if (score > bestScore) {
                bestScore = score;
                bestDialect = tempOption;
            }
        }
    }

    // Set the detected or user-provided dialect options
    dialect = bestDialect;

    // Optionally log the detected or user-set options
    // std::cout << "Detected Dialect: "
    //           << "Delimiter: " << dialect.delimiter
    //           << ", Quote: " << dialect.quoteChar
    //           << ", Escape: " << dialect.escapeChar
    //           << std::endl;
}

//TODO: solve quote that is not used.