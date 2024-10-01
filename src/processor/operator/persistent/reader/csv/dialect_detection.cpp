#include "processor/operator/persistent/reader/csv/dialect_detection.h"

using namespace kuzu::common;

std::vector<DialectOption> generateDialectOptions(const DialectCandidates& candidates,
    const CSVOption& option) {
    std::vector<DialectOption> options;

    constexpr std::array delimiters =
        option.setDelim ? {option.delimiter} : candidates.delimiters;
    constexpr std::array quoteChars =
        option.setQuote ? {option.quoteChar} : candidates.quoteChars;
    constexpr std::array escapeChars =
        option.setEscape ? {option.escapeChar} : candidates.escapeChars;

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
