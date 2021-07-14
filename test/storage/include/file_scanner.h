#pragma once

#include <memory>

#include "src/common/include/file_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class FileScanner {

public:
    FileScanner(string fName) {
        int fd = FileUtils::openFile(fName, O_RDONLY);
        auto length = FileUtils::getFileSize(fd);
        buffer = make_unique<char[]>(length);
        FileUtils::readFromFile(fd, buffer.get(), length, 0);
        FileUtils::closeFile(fd);
    }

protected:
    unique_ptr<char[]> buffer;
};

} // namespace storage
} // namespace graphflow
