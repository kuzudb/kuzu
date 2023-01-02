#include "common/file_utils.h"

#include "common/exception.h"
#include "common/utils.h"

using namespace std;

namespace kuzu {
namespace common {

unique_ptr<FileInfo> FileUtils::openFile(const string& path, int flags) {
    int fd = open(path.c_str(), flags, 0644);
    if (fd == -1) {
        throw Exception("Cannot open file: " + path);
    }
    return make_unique<FileInfo>(path, fd);
}

void FileUtils::closeFile(int fd) {
    if (fd != -1) {
        close(fd);
    }
}

void FileUtils::writeToFile(
    FileInfo* fileInfo, uint8_t* buffer, uint64_t numBytes, uint64_t offset) {
    auto fileSize = getFileSize(fileInfo->fd);
    if (fileSize == -1) {
        throw Exception(StringUtils::string_format("File %s not open.", fileInfo->path.c_str()));
    }
    uint64_t remainingNumBytesToWrite = numBytes;
    uint64_t bufferOffset = 0;
    // Split large writes to 1GB at a time
    uint64_t maxBytesToWriteAtOnce = 1ull << 30; // 1ull << 30 = 1G
    while (remainingNumBytesToWrite > 0) {
        uint64_t numBytesToWrite = min(remainingNumBytesToWrite, maxBytesToWriteAtOnce);
        uint64_t numBytesWritten =
            pwrite(fileInfo->fd, buffer + bufferOffset, numBytesToWrite, offset);
        if (numBytesWritten != numBytesToWrite) {
            throw Exception(StringUtils::string_format(
                "Cannot write to file. path: %s fileDescriptor: %d offsetToWrite: %llu "
                "numBytesToWrite: %llu numBytesWritten: %llu",
                fileInfo->path.c_str(), fileInfo->fd, offset, numBytesToWrite, numBytesWritten));
        }
        remainingNumBytesToWrite -= numBytesWritten;
        offset += numBytesWritten;
        bufferOffset += numBytesWritten;
    }
}

void FileUtils::overwriteFile(const string& from, const string& to) {
    if (!fileOrPathExists(from) || !fileOrPathExists(to))
        return;
    error_code errorCode;
    if (!filesystem::copy_file(from, to, filesystem::copy_options::overwrite_existing, errorCode)) {
        throw Exception(StringUtils::string_format("Error copying file %s to %s.  ErrorMessage: %s",
            from.c_str(), to.c_str(), errorCode.message().c_str()));
    }
}

void FileUtils::readFromFile(
    FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) {
    auto numBytesRead = pread(fileInfo->fd, buffer, numBytes, position);
    if (numBytesRead != numBytes && getFileSize(fileInfo->fd) != position + numBytesRead) {
        throw Exception(
            StringUtils::string_format("Cannot read from file: %s fileDescriptor: %d "
                                       "numBytesRead: %llu numBytesToRead: %llu position: %llu",
                fileInfo->path.c_str(), fileInfo->fd, numBytesRead, numBytes, position));
    }
}

void FileUtils::createDir(const string& dir) {
    if (filesystem::exists(dir)) {
        throw Exception(StringUtils::string_format("Directory %s already exists.", dir.c_str()));
    }
    if (!filesystem::create_directory(dir)) {
        throw Exception(StringUtils::string_format(
            "Directory %s cannot be created. Check if it exists and remove it.", dir.c_str()));
    }
}

void FileUtils::removeDir(const string& dir) {
    error_code removeErrorCode;
    if (!fileOrPathExists(dir))
        return;
    if (!filesystem::remove_all(dir, removeErrorCode)) {
        throw Exception(
            StringUtils::string_format("Error removing directory %s.  Error Message: %s",
                dir.c_str(), removeErrorCode.message().c_str()));
    }
}

void FileUtils::renameFileIfExists(const string& oldName, const string& newName) {
    // If the oldName file doesn't exist, this function does not do anything.
    if (!fileOrPathExists(oldName))
        return;
    error_code errorCode;
    filesystem::rename(oldName, newName, errorCode);
    if (errorCode.value() != 0) {
        throw Exception(
            StringUtils::string_format("Error replacing file %s to %s.  ErrorMessage: %s",
                oldName.c_str(), newName.c_str(), errorCode.message().c_str()));
    }
}

void FileUtils::removeFileIfExists(const string& path) {
    if (!fileOrPathExists(path))
        return;
    if (remove(path.c_str()) != 0) {
        throw Exception(StringUtils::string_format(
            "Error removing directory or file %s.  Error Message: ", path.c_str()));
    }
}

void FileUtils::truncateFileToEmpty(FileInfo* fileInfo) {
    ftruncate(fileInfo->fd, 0);
}

} // namespace common
} // namespace kuzu
