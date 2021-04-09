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

class CSVReader {

public:
    CSVReader(const string& fname, const char tokenSeparator, uint64_t blockId);
    ~CSVReader();

    bool hasNextLine();
    bool hasNextToken();
    void skipLine();
    bool skipTokenIfNull();
    void skipToken();

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