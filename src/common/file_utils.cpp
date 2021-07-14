#include "src/common/include/file_utils.h"

#include <stdexcept>

using namespace std;

namespace graphflow {
namespace common {

int FileUtils::openFile(const string& path, int flags) {
    int fd = open(path.c_str(), flags, 0644);
    if (fd == -1) {
        throw invalid_argument("Cannot open file " + path);
    }
    return fd;
}

void FileUtils::closeFile(int fd) {
    if (fd != -1) {
        close(fd);
    }
}

void FileUtils::writeToFile(int fd, void* buffer, int64_t numBytes, uint64_t offset) {
    auto fileSize = getFileSize(fd);
    if (fileSize == -1) {
        throw invalid_argument("File not open.");
    }
    if (pwrite(fd, buffer, numBytes, offset) != numBytes) {
        throw invalid_argument("Cannot write to file.");
    }
}

void FileUtils::readFromFile(int fd, void* buffer, int64_t numBytes, uint64_t position) {
    auto numBytesRead = pread(fd, buffer, numBytes, position);
    if (numBytesRead != numBytes && getFileSize(fd) != position + numBytesRead) {
        throw invalid_argument("Cannot read from file. numBytesRead: " + to_string(numBytesRead) +
                               " numBytesToRead: " + to_string(numBytes));
    }
}
} // namespace common
} // namespace graphflow
