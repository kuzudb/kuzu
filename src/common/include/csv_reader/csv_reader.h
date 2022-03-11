#pragma once

#include <fstream>

#include "src/common/include/configs.h"
#include "src/common/types/include/literal.h"
#include "src/common/types/include/types_include.h"

using namespace std;

namespace graphflow {
namespace common {

struct CSVReaderConfig {
    CSVReaderConfig()
        : escapeChar{LoaderConfig::DEFAULT_ESCAPE_CHAR},
          tokenSeparator{LoaderConfig::DEFAULT_TOKEN_SEPARATOR},
          quoteChar{LoaderConfig::DEFAULT_QUOTE_CHAR},
          listBeginChar{LoaderConfig::DEFAULT_LIST_BEGIN_CHAR},
          listEndChar{LoaderConfig::DEFAULT_LIST_END_CHAR} {}

    char escapeChar;
    char tokenSeparator;
    char quoteChar;
    char listBeginChar;
    char listEndChar;
};

// todo(Guodong): we should add a csv reader test to test edge cases and error messages.
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
    Literal getList(DataType childDataType);

private:
    void openFile(const string& fName);
    void setNextTokenIsProcessed();

private:
    FILE* fd;
    const CSVReaderConfig& config;
    // todo(Guodong): should `nextLineIsNotProcessed` be renamed as `prevLineIsNotProcessed`?
    bool nextLineIsNotProcessed, isEndOfBlock, nextTokenIsNotProcessed;
    char* line;
    size_t lineCapacity, lineLen;
    int64_t linePtrStart, linePtrEnd;
    size_t readingBlockStartOffset, readingBlockEndOffset;
    uint64_t nextTokenLen;
};

} // namespace common
} // namespace graphflow
