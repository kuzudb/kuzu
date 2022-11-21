#pragma once

#include <fstream>

#include "common/configs.h"
#include "common/types/literal.h"
#include "common/types/types_include.h"

using namespace std;

namespace spdlog {
class logger;
}

namespace kuzu {
namespace common {

struct CSVReaderConfig {
    CSVReaderConfig()
        : escapeChar{CopyCSVConfig::DEFAULT_ESCAPE_CHAR},
          tokenSeparator{CopyCSVConfig::DEFAULT_TOKEN_SEPARATOR},
          quoteChar{CopyCSVConfig::DEFAULT_QUOTE_CHAR},
          listBeginChar{CopyCSVConfig::DEFAULT_LIST_BEGIN_CHAR},
          listEndChar{CopyCSVConfig::DEFAULT_LIST_END_CHAR},
          hasHeader{CopyCSVConfig::DEFAULT_HAS_HEADER} {}

    char escapeChar;
    char tokenSeparator;
    char quoteChar;
    char listBeginChar;
    char listEndChar;
    bool hasHeader;
};

struct CSVDescription {
    CSVDescription(const string filePath, const CSVReaderConfig csvReaderConfig)
        : filePath{move(filePath)}, csvReaderConfig{move(csvReaderConfig)} {}
    const string filePath;
    const CSVReaderConfig csvReaderConfig;
};

// TODO(Guodong): we should add a csv reader test to test edge cases and error messages.
// Iterator-like interface to read one block in a CSV file line-by-line while parsing into primitive
// dataTypes.
class CSVReader {

public:
    // Initializes to read a block in file.
    CSVReader(const string& fname, const CSVReaderConfig& csvReaderConfig, uint64_t blockId);
    // Initializes to read the complete file.
    CSVReader(const string& fname, const CSVReaderConfig& csvReaderConfig);
    // Initializes to read a part of a line.
    CSVReader(
        char* line, uint64_t lineLen, int64_t linePtrStart, const CSVReaderConfig& csvReaderConfig);

    ~CSVReader();

    // returns true if there are more lines to be parsed in a block of a CSV file, else false.
    bool hasNextLine();
    // returns true if the currently-pointed to line has more data to be parsed, else false.
    bool hasNextToken();
    bool hasNextTokenOrError();
    // Marks the currently-pointed to line as processed. hasNextLine() has to be called the move the
    // iterator to the next line.
    void skipLine();
    // Marks the currently-pointed to token as processed. hasNextToken() has to be called the move
    // the iterator to the next token of the line.
    void skipToken();
    // skips the token only if it is empty and returns the result of operation.
    bool skipTokenIfNull();

    // reads the data currently pointed to by the cursor and returns the value in a specific format.
    int64_t getInt64();
    double_t getDouble();
    uint8_t getBoolean();
    char* getString();
    date_t getDate();
    timestamp_t getTimestamp();
    interval_t getInterval();
    Literal getList(const DataType& dataType);

private:
    void openFile(const string& fName);
    void setNextTokenIsProcessed();

private:
    FILE* fd;
    const CSVReaderConfig& config;
    shared_ptr<spdlog::logger> logger;
    bool nextLineIsNotProcessed, isEndOfBlock, nextTokenIsNotProcessed;
    char* line;
    size_t lineCapacity, lineLen;
    int64_t linePtrStart, linePtrEnd;
    size_t readingBlockStartOffset, readingBlockEndOffset;
    uint64_t nextTokenLen;
};

} // namespace common
} // namespace kuzu
