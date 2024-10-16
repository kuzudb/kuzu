#pragma once

#include "common/constants.h"
#include "common/copy_constructors.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace common {

struct CSVOption {
    // TODO(Xiyang): Add newline character option and delimiter can be a string.
    char escapeChar;
    char delimiter;
    char quoteChar;
    bool hasHeader;
    uint64_t skipNum;
    uint64_t sampleSize;
    bool allowUnbracedList;
    bool ignoreErrors;

    bool autoDetection;
    bool setEscape;
    bool setDelim;
    bool setQuote;

    CSVOption()
        : escapeChar{CopyConstants::DEFAULT_CSV_ESCAPE_CHAR},
          delimiter{CopyConstants::DEFAULT_CSV_DELIMITER},
          quoteChar{CopyConstants::DEFAULT_CSV_QUOTE_CHAR},
          hasHeader{CopyConstants::DEFAULT_CSV_HAS_HEADER},
          skipNum{CopyConstants::DEFAULT_CSV_SKIP_NUM},
          sampleSize{CopyConstants::DEFAULT_CSV_TYPE_DEDUCTION_SAMPLE_SIZE},
          allowUnbracedList{CopyConstants::DEFAULT_CSV_ALLOW_UNBRACED_LIST},
          ignoreErrors(CopyConstants::DEFAULT_IGNORE_ERRORS),
          autoDetection{CopyConstants::DEFAULT_CSV_AUTO_DETECT},
          setEscape{CopyConstants::DEFAULT_CSV_SET_DIALECT},
          setDelim{CopyConstants::DEFAULT_CSV_SET_DIALECT},
          setQuote{CopyConstants::DEFAULT_CSV_SET_DIALECT} {}

    EXPLICIT_COPY_DEFAULT_MOVE(CSVOption);

    // TODO: COPY FROM and COPY TO should support transform special options, like '\'.
    std::string toCypher() const {
        std::string header = hasHeader ? "true" : "false";
        std::string result = "(header=" + header + ")";
        // Add the escape parameter IFF setEscape is true
        if (setEscape) {
            result += stringFormat(", escape='\\{}'", escapeChar);
        }
        if (setDelim) {
            result += stringFormat(", delim='{}'", delimiter);
        }
        if (setQuote) {
            result += stringFormat(", quote='\\{}'", quoteChar);
        }

        return result;
    }

    // Explicit copy constructor
    CSVOption(const CSVOption& other)
        : escapeChar{other.escapeChar}, delimiter{other.delimiter}, quoteChar{other.quoteChar},
          hasHeader{other.hasHeader}, skipNum{other.skipNum},
          sampleSize{other.sampleSize == 0 ?
                         CopyConstants::DEFAULT_CSV_TYPE_DEDUCTION_SAMPLE_SIZE :
                         other.sampleSize}, // Set to DEFAULT_CSV_TYPE_DEDUCTION_SAMPLE_SIZE if
                                            // sampleSize is 0
          allowUnbracedList{other.allowUnbracedList}, ignoreErrors{other.ignoreErrors},
          autoDetection{other.autoDetection}, setEscape{other.setEscape}, setDelim{other.setDelim},
          setQuote{other.setQuote} {}
};

struct CSVReaderConfig {
    CSVOption option;
    bool parallel;

    CSVReaderConfig() : option{}, parallel{CopyConstants::DEFAULT_CSV_PARALLEL} {}
    EXPLICIT_COPY_DEFAULT_MOVE(CSVReaderConfig);

    static CSVReaderConfig construct(const std::unordered_map<std::string, common::Value>& options);

private:
    CSVReaderConfig(const CSVReaderConfig& other)
        : option{other.option.copy()}, parallel{other.parallel} {}
};

} // namespace common
} // namespace kuzu
