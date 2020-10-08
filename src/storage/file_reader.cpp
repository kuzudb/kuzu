#include "src/storage/include/file_reader.h"

#include <fstream>

using namespace graphflow::common;

namespace graphflow {
namespace storage {

FileReader::FileReader(const std::string &fname, const char tokenSeparator)
    : tokenSeparator(tokenSeparator) {
    f = std::make_unique<std::ifstream>(fname, std::ifstream::in);
}

bool FileReader::hasMoreTokens() {
    return !f->eof();
}

void FileReader::updateNext() {
    f->get(next);
}

void FileReader::skipLine() {
    while (!isLineSeparator()) {
        updateNext();
    }
    updateNext();
}

std::unique_ptr<std::string> FileReader::getLine() {
    auto val = std::make_unique<std::string>();
    while (!isLineSeparator()) {
        val->push_back(next);
        updateNext();
    }
    updateNext();
    return val;
}

} // namespace storage
} // namespace graphflow
