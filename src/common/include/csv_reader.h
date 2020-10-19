#ifndef GRAPHFLOW_STORAGE_FILEREADER_H_
#define GRAPHFLOW_STORAGE_FILEREADER_H_

#include <stdio.h>
#include <string.h>

#include <cmath>
#include <iostream>
#include <memory>
#include <string>

#include "src/common/include/types.h"

using namespace graphflow::common;

namespace graphflow {
namespace common {

class CSVReader {

    std::unique_ptr<std::ifstream> f;
    const char tokenSeparator;
    char next;

public:
    CSVReader(const std::string &fname, const char tokenSeparator);

    bool hasMoreTokens();
    void skipLine();

    std::unique_ptr<std::string> getLine();

private:
    void updateNext();
    inline bool isLineSeparator() { return '\n' == next; }
    inline bool isTokenSeparator() { return isLineSeparator() || next == tokenSeparator; }
};

} // namespace common
} // namespace graphflow

#endif
