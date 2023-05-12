#include "common/file_utils.h"

#include "common/exception.h"
#include "common/string_utils.h"
#include "common/utils.h"
#include <glob/glob.h>

#ifdef _WIN32
#include <windows.h>
#endif

namespace kuzu {
namespace common {

std::unique_ptr<FileInfo> FileUtils::openFile(const std::string& path, int flags) {
    #if defined(_WIN32)
    int fd = open(path.c_str(), flags | _O_BINARY, _S_IREAD | _S_IWRITE);
    #else
    int fd = open(path.c_str(), flags, 0644);
    #endif
    if (fd == -1) {
        throw Exception("Cannot open file: " + path);
    }
    return std::make_unique<FileInfo>(path, fd);
}

void FileUtils::createFileWithSize(const std::string& path, uint64_t size) {
    auto fileInfo = common::FileUtils::openFile(path, O_WRONLY | O_CREAT);
    common::FileUtils::truncateFileToSize(fileInfo.get(), size);
    fileInfo.reset();
}

void FileUtils::writeToFile(
    FileInfo* fileInfo, uint8_t* buffer, uint64_t numBytes, uint64_t offset) {
    auto fileSize = getFileSize(fileInfo->fd);
    if (fileSize == -1) {
        throw Exception(StringUtils::string_format("File {} not open.", fileInfo->path));
    }
    uint64_t remainingNumBytesToWrite = numBytes;
    uint64_t bufferOffset = 0;
    // Split large writes to 1GB at a time
    uint64_t maxBytesToWriteAtOnce = 1ull << 30; // 1ull << 30 = 1G
    while (remainingNumBytesToWrite > 0) {
        uint64_t numBytesToWrite = std::min(remainingNumBytesToWrite, maxBytesToWriteAtOnce);

        #if defined(_WIN32)
        auto handle = (HANDLE)_get_osfhandle(fileInfo->fd);
        DWORD numBytesWritten;
        OVERLAPPED overlapped{0,0,0.0};
        overlapped.Offset = offset & 0xffffffff;
        overlapped.OffsetHigh = offset >> 32;
        if (!WriteFile(handle, buffer + bufferOffset, numBytesToWrite, &numBytesWritten, &overlapped)) {
            auto error = GetLastError();
            throw Exception(StringUtils::string_format(
                "Cannot write to file. path: {} fileDescriptor: {} offsetToWrite: {} "
                "numBytesToWrite: {} numBytesWritten: {}. Error {}: {}.",
                fileInfo->path, fileInfo->fd, offset, numBytesToWrite, numBytesWritten,
                error, std::system_category().message(error)));
        }
        #else
        uint64_t numBytesWritten =
            pwrite(fileInfo->fd, buffer + bufferOffset, numBytesToWrite, offset);
        if (numBytesWritten != numBytesToWrite) {
            throw Exception(StringUtils::string_format(
                "Cannot write to file. path: {} fileDescriptor: {} offsetToWrite: {} "
                "numBytesToWrite: {} numBytesWritten: {}.",
                fileInfo->path, fileInfo->fd, offset, numBytesToWrite, numBytesWritten));
        }
        #endif
        remainingNumBytesToWrite -= numBytesWritten;
        offset += numBytesWritten;
        bufferOffset += numBytesWritten;
    }
}

void FileUtils::overwriteFile(const std::string& from, const std::string& to) {
    if (!fileOrPathExists(from) || !fileOrPathExists(to))
        return;
    std::error_code errorCode;
    if (!std::filesystem::copy_file(
            from, to, std::filesystem::copy_options::overwrite_existing, errorCode)) {
        throw Exception(StringUtils::string_format(
            "Error copying file {} to {}.  ErrorMessage: {}", from, to, errorCode.message()));
    }
}

void FileUtils::readFromFile(
    FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) {
    #if defined(_WIN32)
    auto handle = (HANDLE)_get_osfhandle(fileInfo->fd);
    DWORD numBytesRead;
    OVERLAPPED overlapped{0,0,0.0};
    overlapped.Offset = position & 0xffffffff;
    overlapped.OffsetHigh = position >> 32;
    if (!ReadFile(handle, buffer, numBytes, &numBytesRead, &overlapped)) {
        auto error = GetLastError();
        throw Exception(
            StringUtils::string_format("Cannot read from file: {} fileDescriptor: {} "
                                       "numBytesRead: {} numBytesToRead: {} position: {}. Error {}: {}",
            fileInfo->path, fileInfo->fd, numBytesRead, numBytes, position,
            error, std::system_category().message(error)));
    }
    #else
    auto numBytesRead = pread(fileInfo->fd, buffer, numBytes, position);
    #endif
    if (numBytesRead != numBytes && getFileSize(fileInfo->fd) != position + numBytesRead) {
        throw Exception(
            StringUtils::string_format("Cannot read from file: {} fileDescriptor: {} "
                                       "numBytesRead: {} numBytesToRead: {} position: {}",
                fileInfo->path, fileInfo->fd, numBytesRead, numBytes, position));
    }
}

void FileUtils::createDir(const std::string& dir) {
    try {
        if (std::filesystem::exists(dir)) {
            throw Exception(StringUtils::string_format("Directory {} already exists.", dir));
        }
        if (!std::filesystem::create_directory(dir)) {
            throw Exception(StringUtils::string_format(
                "Directory {} cannot be created. Check if it exists and remove it.", dir));
        }
    } catch (std::exception& e) {
        throw Exception(
            StringUtils::string_format("Failed to create directory {} due to: {}", dir, e.what()));
    }
}

void FileUtils::removeDir(const std::string& dir) {
    std::error_code removeErrorCode;
    if (!fileOrPathExists(dir))
        return;
    if (!std::filesystem::remove_all(dir, removeErrorCode)) {
        throw Exception(StringUtils::string_format(
            "Error removing directory {}.  Error Message: {}", dir, removeErrorCode.message()));
    }
}

void FileUtils::renameFileIfExists(const std::string& oldName, const std::string& newName) {
    // If the oldName file doesn't exist, this function does not do anything.
    if (!fileOrPathExists(oldName))
        return;
    std::error_code errorCode;
    std::filesystem::rename(oldName, newName, errorCode);
    if (errorCode.value() != 0) {
        throw Exception(
            StringUtils::string_format("Error replacing file {} to {}.  ErrorMessage: {}", oldName,
                newName, errorCode.message()));
    }
}

void FileUtils::removeFileIfExists(const std::string& path) {
    if (!fileOrPathExists(path))
        return;
    if (remove(path.c_str()) != 0) {
        throw Exception(StringUtils::string_format(
            "Error removing directory or file {}.  Error Message: ", path));
    }
}

std::vector<std::string> FileUtils::globFilePath(const std::string& path) {
    std::vector<std::string> result;
    for (auto &resultPath: glob::glob(path)) {
        result.emplace_back(resultPath.string());
    }
    return result;
}

std::vector<std::string> FileUtils::findAllDirectories(const std::string& path) {
    std::vector<std::string> directories;
    directories.push_back(path);
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
        if (entry.is_directory()) {
            directories.push_back(entry.path().string());
        }
    }
    return directories;
}

void FileUtils::truncateFileToSize(FileInfo* fileInfo, uint64_t size) {
    #if defined(_WIN32)
    _chsize_s(fileInfo->fd, size);
    #else
    ftruncate(fileInfo->fd, size);
    #endif
}

} // namespace common
} // namespace kuzu
