#include "processor/operator/persistent/reader/csv/dialect_detection.h"

using namespace kuzu::common;

// Constructor for DialectCandidates to initialize default values
DialectCandidates::DialectCandidates() {
    delimiters = {',', ';', '\t', '|'};
    quoteChars = {'"', '\''};
    escapeChars = {'"', '\\', '\''};
}

// Generates all combinations of dialect options
std::vector<DialectOption> generateDialectOptions(const DialectCandidates& candidates,
    const CSVOption& option) {
    std::vector<DialectOption> options;

    // Determine the valid delimiters, quotes, and escapes based on user input
    std::vector<char> delimiters =
        option.setDelim ? std::vector<char>{option.delimiter} : candidates.delimiters;
    std::vector<char> quoteChars =
        option.setQuote ? std::vector<char>{option.quoteChar} : candidates.quoteChars;
    std::vector<char> escapeChars =
        option.setEscape ? std::vector<char>{option.escapeChar} : candidates.escapeChars;

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
