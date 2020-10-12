#include "src/common/include/csv_reader.h"

#include <fstream>

namespace graphflow {
namespace common {

CSVReader::CSVReader(const std::string &fname, const char tokenSeparator)
    : tokenSeparator(tokenSeparator) {
    f = std::make_unique<std::ifstream>(fname, std::ifstream::in);
}

bool CSVReader::hasMoreTokens() {
    return !f->eof();
}

void CSVReader::updateNext() {
    f->get(next);
}

void CSVReader::skipLine() {
    while (!isLineSeparator()) {
        updateNext();
    }
    updateNext();
}

std::unique_ptr<std::string> CSVReader::getLine() {
    auto val = std::make_unique<std::string>();
    while (!isLineSeparator()) {
        val->push_back(next);
        updateNext();
    }
    updateNext();
    return val;
}

} // namespace common
} // namespace graphflow
