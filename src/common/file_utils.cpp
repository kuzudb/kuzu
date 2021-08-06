#include "src/common/include/file_utils.h"

#include <stdexcept>

#include "src/common/include/utils.h"

using namespace std;

namespace graphflow {
namespace common {

unique_ptr<FileInfo> FileUtils::openFile(const string& path, int flags) {
    int fd = open(path.c_str(), flags, 0644);
    if (fd == -1) {
        throw invalid_argument(StringUtils::string_format("Cannot open file: ", path.c_str()));
    }
    return make_unique<FileInfo>(path, fd);
}

void FileUtils::closeFile(int fd) {
    if (fd != -1) {
        close(fd);
    }
}

void FileUtils::writeToFile(FileInfo* fileInfo, void* buffer, int64_t numBytes, uint64_t offset) {
    auto fileSize = getFileSize(fileInfo->fd);
    if (fileSize == -1) {
        throw invalid_argument(
            StringUtils::string_format("File %s not open.", fileInfo->path.c_str()));
    }
    auto numBytesWritten = pwrite(fileInfo->fd, buffer, numBytes, offset);
    if (numBytesWritten != numBytes) {
        throw invalid_argument(StringUtils::string_format(
            "Cannot write to file. path: %s fileDescriptor: %d offsetToWrite: %d "
            "numBytesToWrite: %d numBytesWritten: %d ",
            fileInfo->path.c_str(), fileInfo->fd, offset, numBytes, numBytesWritten));
    }
}

unique_ptr<uint8_t[]> FileUtils::readFile(FileInfo* fileInfo) {
    auto fileLength = FileUtils::getFileSize(fileInfo->fd);
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
        readFromFile(fileInfo, buffer.get() + bufferOffset, numBytesToRead, position);
        remainingNumBytesToRead -= numBytesToRead;
        position += numBytesToRead;
        bufferOffset += numBytesToRead;
    }
    return move(buffer);
}

void FileUtils::readFromFile(
    FileInfo* fileInfo, void* buffer, int64_t numBytes, uint64_t position) {
    auto numBytesRead = pread(fileInfo->fd, buffer, numBytes, position);
    if (numBytesRead != numBytes && getFileSize(fileInfo->fd) != position + numBytesRead) {
        throw invalid_argument(
            StringUtils::string_format("Cannot read from file: %s fileDescriptor: %d "
                                       "numBytesRead: %d numBytesToRead: %d",
                fileInfo->path.c_str(), fileInfo->fd, numBytesRead, numBytes));
    }
}

void FileUtils::createDir(const string& dir) {
    if (filesystem::exists(dir)) {
        throw runtime_error(
            StringUtils::string_format("Directory %s already exists.", dir.c_str()));
    }
    if (!filesystem::create_directory(dir)) {
        throw runtime_error(StringUtils::string_format(
            "Directory %s cannot be created. Check if it exists and remove it.", dir.c_str()));
    }
}

void FileUtils::removeDir(const string& dir) {
    error_code removeErrorCode;
    if (!fileExists(dir))
        return;
    if (!filesystem::remove_all(dir, removeErrorCode)) {
        throw runtime_error(
            StringUtils::string_format("Error removing directory %s.  Error Message: ", dir.c_str(),
                removeErrorCode.message().c_str()));
    }
}

} // namespace common
} // namespace graphflow
