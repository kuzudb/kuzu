#include "test/storage/include/file_scanners/file_scanner.h"

namespace graphflow {
namespace storage {

FileScanner::FileScanner(const string& fName) {
    int fd = FileUtils::openFile(fName, O_RDONLY);
    auto length = FileUtils::getFileSize(fd);
    buffer = make_unique<uint8_t[]>(length);
    FileUtils::readFromFile(fd, buffer.get(), length, 0);
    FileUtils::closeFile(fd);
}

} // namespace storage
} // namespace graphflow
