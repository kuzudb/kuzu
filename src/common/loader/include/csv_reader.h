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
namespace common {

class CSVReader {

public:
    CSVReader(const string &fname, const char tokenSeparator, uint64_t blockId);
    ~CSVReader();

    bool hasNextLine();
    bool hasNextToken();
    void skipLine();
    bool skipTokenIfNull();
    void skipToken();

    unique_ptr<string> getNodeID();
    gfInt_t getInteger();
    gfDouble_t getDouble();
    gfBool_t getBoolean();
    unique_ptr<string> getString();

private:
    FILE *f;
    const char tokenSeparator;
    bool nextLineIsNotProcessed, isEndOfBlock, nextTokenIsNotProcessed;
    char *line;
    size_t lineCapacity, lineLen;
    int64_t linePtrStart, linePtrEnd;
    char *trueVal = (char *)"true";
    char *falseVal = (char *)"false";
    size_t readingBlockIdx;
    size_t readingBlockEndIdx;
};

} // namespace common
} // namespace graphflow