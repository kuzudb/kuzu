#pragma once

#include <fstream>

#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

const string PROPERTY_DATATYPE_SEPARATOR = ":";

// Iterator-like interface to read one block in a CSV file line-by-line while parsing into primitive
// dataTypes.
class CSVReader {

public:
    constexpr static char COMMENT_LINE_CHAR = '#';
    // Initializes to read a block in file.
    CSVReader(const string& fname, const char tokenSeparator, const char quoteChar,
        const char escapeChar, uint64_t blockId);
    // Intializes to read the complete file.
    CSVReader(const string& fname, const char tokenSeparator, const char quoteChar,
        const char escapeChar);

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

private:
    void openFile(const string& fName);
    void setNextTokenIsNotProcessed();

private:
    FILE* fd;
    const char tokenSeparator;
    const char quoteChar;
    const char escapeChar;
    bool nextLineIsNotProcessed = false, isEndOfBlock = false, nextTokenIsNotProcessed = false;
    char* line;
    size_t lineCapacity = 1 << 10, lineLen = 0;
    int64_t linePtrStart = -1l, linePtrEnd = -1l;
    size_t readingBlockIdx;
    size_t readingBlockEndIdx;
    uint64_t nextTokenLen = -1;
};

} // namespace common
} // namespace graphflow
