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

    bool hasMore();
    unique_ptr<string> getNodeID();
    gfInt_t getInteger();
    gfDouble_t getDouble();
    gfBool_t getBoolean();
    unique_ptr<string> getString();
    void skipLine();
    bool skipTokenIfNULL();
    void skipToken();

private:
    void updateNext();

    inline bool isLineSeparator() { return f.eof() || '\n' == next; }
    inline bool isTokenSeparator() { return isLineSeparator() || next == tokenSeparator; }

    void ensureStringBufferCapacity();

private:
    ifstream f;
    const char tokenSeparator;
    char next;
    size_t readingBlockIdx;
    size_t readingBlockEndIdx;
    unique_ptr<char[]> strBuffer;
    size_t strBufferLen, strBufferCapacity;
};

} // namespace common
} // namespace graphflow
