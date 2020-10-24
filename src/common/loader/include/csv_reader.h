#ifndef GRAPHFLOW_COMMON_LOADER_CSV_READER_H_
#define GRAPHFLOW_COMMON_LOADER_CSV_READER_H_

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
    uint64_t getNodeID();
    gfInt_t getInteger();
    gfDouble_t getDouble();
    gfBool_t getBoolean();
    std::unique_ptr<std::string> getString();
    void skipLine();
    bool skipTokenIfNULL();
    void skipToken();
    unique_ptr<string> getLine();

private:
    void updateNext();
    inline bool isLineSeparator() { return '\n' == next; }
    inline bool isTokenSeparator() { return isLineSeparator() || next == tokenSeparator; }

    unique_ptr<ifstream> f;
    const char tokenSeparator;
    char next;
    size_t readingBlockIdx;
    size_t readingBlockEndIdx;
};

} // namespace common
} // namespace graphflow

#endif // GRAPHFLOW_COMMON_LOADER_CSV_READER_H_
