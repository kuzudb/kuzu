#include "common/file_utils.h"

#include "common/exception.h"
#include "common/string_utils.h"
#include "glob/glob.hpp"

#ifdef _WIN32
#include <fileapi.h>
#include <windows.h>
#endif

namespace kuzu {
namespace common {

FileInfo::~FileInfo() {
#ifdef _WIN32
    if (handle != nullptr) {
        CloseHandle((HANDLE)handle);
    }
#else
    if (fd != -1) {
        close(fd);
    }
#endif
}

int64_t FileInfo::getFileSize() {
#ifdef _WIN32
    LARGE_INTEGER size;
    if (!GetFileSizeEx((HANDLE)handle, &size)) {
        auto error = GetLastError();
        throw common::StorageException(
            StringUtils::string_format("Cannot read size of file. path: {} - Error {}: {}", path,
                error, std::system_category().message(error)));
    }
    return size.QuadPart;
#else
    struct stat s;
    if (fstat(fd, &s) == -1) {
        return -1;
    }
    return s.st_size;
#endif
}

std::unique_ptr<FileInfo> FileUtils::openFile(const std::string& path, int flags) {
#if defined(_WIN32)
    auto dwDesiredAccess = 0ul;
    auto dwCreationDisposition = (flags & O_CREAT) ? OPEN_ALWAYS : OPEN_EXISTING;
    auto dwShareMode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
    if (flags & (O_CREAT | O_WRONLY | O_RDWR)) {
        dwDesiredAccess |= GENERIC_WRITE;
    }
    // O_RDONLY is 0 in practice, so flags & (O_RDONLY | O_RDWR) doesn't work.
    if (!(flags & O_WRONLY)) {
        dwDesiredAccess |= GENERIC_READ;
    }

    HANDLE handle = CreateFileA(path.c_str(), dwDesiredAccess, dwShareMode, nullptr,
        dwCreationDisposition, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        throw Exception(StringUtils::string_format("Cannot open file. path: {} - Error {}: {}",
            path, GetLastError(), std::system_category().message(GetLastError())));
    }
    return std::make_unique<FileInfo>(path, handle);
#else
    int fd = open(path.c_str(), flags, 0644);
    if (fd == -1) {
        throw Exception("Cannot open file: " + path);
    }
    return std::make_unique<FileInfo>(path, fd);
#endif
}

void FileUtils::createFileWithSize(const std::string& path, uint64_t size) {
    auto fileInfo = common::FileUtils::openFile(path, O_WRONLY | O_CREAT);
    common::FileUtils::truncateFileToSize(fileInfo.get(), size);
    fileInfo.reset();
}

void FileUtils::writeToFile(
    FileInfo* fileInfo, uint8_t* buffer, uint64_t numBytes, uint64_t offset) {
    auto fileSize = fileInfo->getFileSize();
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
        DWORD numBytesWritten;
        OVERLAPPED overlapped{0, 0, 0, 0};
        overlapped.Offset = offset & 0xffffffff;
        overlapped.OffsetHigh = offset >> 32;
        if (!WriteFile((HANDLE)fileInfo->handle, buffer + bufferOffset, numBytesToWrite,
                &numBytesWritten, &overlapped)) {
            auto error = GetLastError();
            throw Exception(StringUtils::string_format(
                "Cannot write to file. path: {} handle: {} offsetToWrite: {} "
                "numBytesToWrite: {} numBytesWritten: {}. Error {}: {}.",
                fileInfo->path, (intptr_t)fileInfo->handle, offset, numBytesToWrite,
                numBytesWritten, error, std::system_category().message(error)));
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

void FileUtils::copyFile(
    const std::string& from, const std::string& to, std::filesystem::copy_options options) {
    if (!fileOrPathExists(from))
        return;
    std::error_code errorCode;
    if (!std::filesystem::copy_file(from, to, options, errorCode)) {
        throw Exception(StringUtils::string_format(
            "Error copying file {} to {}.  ErrorMessage: {}", from, to, errorCode.message()));
    }
}

void FileUtils::overwriteFile(const std::string& from, const std::string& to) {
    if (!fileOrPathExists(from) || !fileOrPathExists(to))
        return;
    copyFile(from, to, std::filesystem::copy_options::overwrite_existing);
}

void FileUtils::readFromFile(
    FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) {
#if defined(_WIN32)
    DWORD numBytesRead;
    OVERLAPPED overlapped{0, 0, 0, 0};
    overlapped.Offset = position & 0xffffffff;
    overlapped.OffsetHigh = position >> 32;
    if (!ReadFile((HANDLE)fileInfo->handle, buffer, numBytes, &numBytesRead, &overlapped)) {
        auto error = GetLastError();
        throw common::StorageException(StringUtils::string_format(
            "Cannot read from file: {} handle: {} "
            "numBytesRead: {} numBytesToRead: {} position: {}. Error {}: {}",
            fileInfo->path, (intptr_t)fileInfo->handle, numBytesRead, numBytes, position, error,
            std::system_category().message(error)));
    }
    if (numBytesRead != numBytes && fileInfo->getFileSize() != position + numBytesRead) {
        throw common::StorageException(
            StringUtils::string_format("Cannot read from file: {} handle: {} "
                                       "numBytesRead: {} numBytesToRead: {} position: {}",
                fileInfo->path, (intptr_t)fileInfo->handle, numBytesRead, numBytes, position));
    }
#else
    auto numBytesRead = pread(fileInfo->fd, buffer, numBytes, position);
    if (numBytesRead != numBytes && fileInfo->getFileSize() != position + numBytesRead) {
        throw Exception(
            StringUtils::string_format("Cannot read from file: {} fileDescriptor: {} "
                                       "numBytesRead: {} numBytesToRead: {} position: {}",
                fileInfo->path, fileInfo->fd, numBytesRead, numBytes, position));
    }
#endif
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

void FileUtils::createDirIfNotExists(const std::string& path) {
    if (!fileOrPathExists(path)) {
        createDir(path);
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
    for (auto& resultPath : glob::glob(path)) {
        result.emplace_back(resultPath.string());
    }
    return result;
}

void FileUtils::truncateFileToSize(FileInfo* fileInfo, uint64_t size) {
#if defined(_WIN32)
    auto offsetHigh = (LONG)(size >> 32);
    LONG* offsetHighPtr = NULL;
    if (offsetHigh > 0)
        offsetHighPtr = &offsetHigh;
    if (SetFilePointer((HANDLE)fileInfo->handle, size & 0xffffffff, offsetHighPtr, FILE_BEGIN) ==
        INVALID_SET_FILE_POINTER) {
        auto error = GetLastError();
        throw Exception(
            StringUtils::string_format("Cannot set file pointer for file: {} handle: {} "
                                       "new position: {}. Error {}: {}",
                fileInfo->path, (intptr_t)fileInfo->handle, size, error,
                std::system_category().message(error)));
    }
    if (!SetEndOfFile((HANDLE)fileInfo->handle)) {
        auto error = GetLastError();
        throw Exception(StringUtils::string_format("Cannot truncate file: {} handle: {} "
                                                   "size: {}. Error {}: {}",
            fileInfo->path, (intptr_t)fileInfo->handle, size, error,
            std::system_category().message(error)));
    }
#else
    ftruncate(fileInfo->fd, size);
#endif
}

} // namespace common
} // namespace kuzu
