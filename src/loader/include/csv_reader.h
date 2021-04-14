#pragma once

#include <stdio.h>
#include <string.h>

#include <cmath>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>

#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace loader {

// Iterator-like interface to read one block in a CSV file line-by-line while parsing into primitive
// dataTypes.
class CSVReader {

public:
    CSVReader(const string& fname, const char tokenSeparator, uint64_t blockId);
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
    int32_t getInt32();
    double_t getDouble();
    uint8_t getBoolean();
    char* getString();

private:
    FILE* f;
    const char tokenSeparator;
    bool nextLineIsNotProcessed, isEndOfBlock, nextTokenIsNotProcessed;
    char* line;
    size_t lineCapacity, lineLen;
    int64_t linePtrStart, linePtrEnd;
    size_t readingBlockIdx;
    size_t readingBlockEndIdx;
};

} // namespace loader
} // namespace graphflow