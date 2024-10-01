#include "processor/operator/persistent/reader/csv/dialect_detection.h"

using namespace kuzu::common;

DialectCandidates::DialectCandidates() {
    delimiters = CopyConstants::DEFAULT_CSV_DELIMITER_SEARCH_SPACE;
    quoteChars = CopyConstants::DEFAULT_CSV_QUOTE_SEARCH_SPACE;
    escapeChars = CopyConstants::DEFAULT_CSV_ESCAPE_SEARCH_SPACE;
}

std::vector<DialectOption> generateDialectOptions(const DialectCandidates& candidates,
    const CSVOption& option) {
    std::vector<DialectOption> options;

    std::vector<char> delimiters =
        option.setDelim ? std::vector<char>{option.delimiter} : candidates.delimiters;
    std::vector<char> quoteChars =
        option.setQuote ? std::vector<char>{option.quoteChar} : candidates.quoteChars;
    std::vector<char> escapeChars =
        option.setEscape ? std::vector<char>{option.escapeChar} : candidates.escapeChars;

    for (auto& delim : delimiters) {
        for (auto& quote : quoteChars) {
            for (auto& escape : escapeChars) {
                DialectOption option{delim, quote, escape};
                options.push_back(option);
            }
        }
    }
    return options;
}
