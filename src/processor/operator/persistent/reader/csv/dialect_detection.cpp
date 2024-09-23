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
std::vector<DialectOption> generateDialectOptions(const DialectCandidates& candidates, const CSVOption& option) {
    std::vector<DialectOption> options;

    // Determine the valid delimiters, quotes, and escapes based on user input
    std::vector<char> delimiters = option.setDelim ? std::vector<char>{option.delimiter} : candidates.delimiters;
    std::vector<char> quoteChars = option.setQuote ? std::vector<char>{option.quoteChar} : candidates.quoteChars;
    std::vector<char> escapeChars = option.setEscape ? std::vector<char>{option.escapeChar} : candidates.escapeChars;

    // Generate only the necessary combinations based on the adjusted search space
    for (char delim : delimiters) {
        for (char quote : quoteChars) {
            for (char escape : escapeChars) {
                DialectOption option{delim, quote, escape};
                options.push_back(option);
            }
        }
    }
    return options;
}

// // Scores the consistency of a dialect based on the most frequent number of fields per row
// int scoreDialect(const std::vector<int>& fieldCounts) {
//     if (fieldCounts.empty()) {
//         return 0;
//     }

//     // Map to count occurrences of each field count
//     std::unordered_map<int, int> fieldCountFrequency;
//     for (int count : fieldCounts) {
//         fieldCountFrequency[count]++;
//     }

//     // Find the most frequent field count (mode)
//     int mostFrequentFieldCount = 0;
//     int highestFrequency = 0;
//     for (const auto& entry : fieldCountFrequency) {
//         if (entry.second > highestFrequency) {
//             mostFrequentFieldCount = entry.first;
//             highestFrequency = entry.second;
//         }
//     }

//     // Calculate the number of rows that match the most frequent field count
//     int consistentRows = std::count(fieldCounts.begin(), fieldCounts.end(), mostFrequentFieldCount);

//     return consistentRows;
// }