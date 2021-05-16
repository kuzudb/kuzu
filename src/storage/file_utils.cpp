#include "src/storage/include/file_utils.h"

#include <sys/stat.h>

#include <cstring>
#include <stdexcept>

using namespace std;

namespace graphflow {
namespace storage {

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

int64_t FileUtils::getFileSize(int fd) {
    struct stat s;
    if (fstat(fd, &s) == -1) {
        return -1;
    }
    return s.st_size;
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
    if (pread(fd, buffer, numBytes, position) != numBytes) {
        throw invalid_argument("Cannot read from file.");
    }
}
} // namespace storage
} // namespace graphflow
