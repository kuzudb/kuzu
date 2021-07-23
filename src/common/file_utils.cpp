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

unique_ptr<uint8_t[]> FileUtils::readFile(int fd) {
    auto fileLength = FileUtils::getFileSize(fd);
    unique_ptr<uint8_t[]> buffer = make_unique<uint8_t[]>(fileLength);
    // Maximum bytes to read from a file, when reading a large file in pieces. 1ull << 30 = 1G
    uint64_t maxBytesToReadFromFileAtOnce = 1ull << 30;
    uint64_t remainingNumBytesToRead = fileLength;
    uint64_t position = 0;
    uint64_t bufferOffset = 0;
    while (remainingNumBytesToRead > 0) {
        uint64_t numBytesToRead = remainingNumBytesToRead > maxBytesToReadFromFileAtOnce ?
                                      maxBytesToReadFromFileAtOnce :
                                      remainingNumBytesToRead;
        readFromFile(fd, buffer.get() + bufferOffset, numBytesToRead, position);
        remainingNumBytesToRead -= numBytesToRead;
        position += numBytesToRead;
        bufferOffset += numBytesToRead;
    }
    return move(buffer);
}

void FileUtils::readFromFile(int fd, void* buffer, int64_t numBytes, uint64_t position) {
    auto numBytesRead = pread(fd, buffer, numBytes, position);
    if (numBytesRead != numBytes && getFileSize(fd) != position + numBytesRead) {
        throw invalid_argument("Cannot read from file. numBytesRead: " + to_string(numBytesRead) +
                               " numBytesToRead: " + to_string(numBytes));
    }
}

void FileUtils::createDir(const string& dir) {
    if (filesystem::exists(dir)) {
        throw runtime_error("Directory already exists.");
    }
    if (!filesystem::create_directory(dir)) {
        throw runtime_error("Directory cannot be created. Check if it exists and remove it.");
    }
}

void FileUtils::removeDir(const string& dir) {
    error_code removeErrorCode;
    if (!fileExists(dir))
        return;
    if (!filesystem::remove_all(dir, removeErrorCode)) {
        throw runtime_error("Remove directory error: " + removeErrorCode.message());
    }
}

} // namespace common
} // namespace graphflow
