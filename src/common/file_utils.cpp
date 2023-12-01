#include "common/file_utils.h"

#include <cstring>

#include "common/exception/storage.h"
#include "common/string_format.h"
#include "common/system_message.h"
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
        throw StorageException(stringFormat("Cannot read size of file. path: {} - Error {}: {}",
            path, error, systemErrMessage(error)));
    }
    return size.QuadPart;
#else
    struct stat s;
    if (fstat(fd, &s) == -1) {
        throw StorageException(stringFormat(
            "Cannot read size of file. path: {} - Error {}: {}", path, errno, posixErrMessage()));
    }
    return s.st_size;
#endif
}

std::unique_ptr<FileInfo> FileUtils::openFile(
    const std::string& path, int flags, FileLockType lock_type) {
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
        throw Exception(stringFormat("Cannot open file. path: {} - Error {}: {}", path,
            GetLastError(), std::system_category().message(GetLastError())));
    }
    if (lock_type != FileLockType::NO_LOCK) {
        DWORD dwFlags = lock_type == FileLockType::READ_LOCK ?
                            LOCKFILE_FAIL_IMMEDIATELY :
                            LOCKFILE_FAIL_IMMEDIATELY | LOCKFILE_EXCLUSIVE_LOCK;
        OVERLAPPED overlapped = {0};
        overlapped.Offset = 0;
        BOOL rc = LockFileEx(handle, dwFlags, 0, 0, 0, &overlapped);
        if (!rc) {
            throw Exception("Could not set lock on file : " + path);
        }
    }
    return std::make_unique<FileInfo>(path, handle);
#else
    int fd = open(path.c_str(), flags, 0644);
    if (fd == -1) {
        throw Exception("Cannot open file: " + path);
    }
    if (lock_type != FileLockType::NO_LOCK) {
        struct flock fl;
        memset(&fl, 0, sizeof fl);
        fl.l_type = lock_type == FileLockType::READ_LOCK ? F_RDLCK : F_WRLCK;
        fl.l_whence = SEEK_SET;
        fl.l_start = 0;
        fl.l_len = 0;
        int rc = fcntl(fd, F_SETLK, &fl);
        if (rc == -1) {
            throw Exception("Could not set lock on file : " + path);
        }
    }
    return std::make_unique<FileInfo>(path, fd);
#endif
}

void FileUtils::writeToFile(
    FileInfo* fileInfo, const uint8_t* buffer, uint64_t numBytes, uint64_t offset) {
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
            throw Exception(
                stringFormat("Cannot write to file. path: {} handle: {} offsetToWrite: {} "
                             "numBytesToWrite: {} numBytesWritten: {}. Error {}: {}.",
                    fileInfo->path, (intptr_t)fileInfo->handle, offset, numBytesToWrite,
                    numBytesWritten, error, std::system_category().message(error)));
        }
#else
        uint64_t numBytesWritten =
            pwrite(fileInfo->fd, buffer + bufferOffset, numBytesToWrite, offset);
        if (numBytesWritten != numBytesToWrite) {
            // LCOV_EXCL_START
            throw Exception(
                stringFormat("Cannot write to file. path: {} fileDescriptor: {} offsetToWrite: {} "
                             "numBytesToWrite: {} numBytesWritten: {}. Error: {}",
                    fileInfo->path, fileInfo->fd, offset, numBytesToWrite, numBytesWritten,
                    posixErrMessage()));
            // LCOV_EXCL_STOP
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
        // LCOV_EXCL_START
        throw Exception(stringFormat(
            "Error copying file {} to {}.  ErrorMessage: {}", from, to, errorCode.message()));
        // LCOV_EXCL_STOP
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
        throw StorageException(
            stringFormat("Cannot read from file: {} handle: {} "
                         "numBytesRead: {} numBytesToRead: {} position: {}. Error {}: {}",
                fileInfo->path, (intptr_t)fileInfo->handle, numBytesRead, numBytes, position, error,
                std::system_category().message(error)));
    }
    if (numBytesRead != numBytes && fileInfo->getFileSize() != position + numBytesRead) {
        throw StorageException(stringFormat("Cannot read from file: {} handle: {} "
                                            "numBytesRead: {} numBytesToRead: {} position: {}",
            fileInfo->path, (intptr_t)fileInfo->handle, numBytesRead, numBytes, position));
    }
#else
    auto numBytesRead = pread(fileInfo->fd, buffer, numBytes, position);
    if (numBytesRead != numBytes && fileInfo->getFileSize() != position + numBytesRead) {
        // LCOV_EXCL_START
        throw Exception(stringFormat("Cannot read from file: {} fileDescriptor: {} "
                                     "numBytesRead: {} numBytesToRead: {} position: {}",
            fileInfo->path, fileInfo->fd, numBytesRead, numBytes, position));
        // LCOV_EXCL_STOP
    }
#endif
}

void FileUtils::createDir(const std::string& dir) {
    try {
        if (std::filesystem::exists(dir)) {
            // LCOV_EXCL_START
            throw Exception(stringFormat("Directory {} already exists.", dir));
            // LCOV_EXCL_STOP
        }
        if (!std::filesystem::create_directory(dir)) {
            // LCOV_EXCL_START
            throw Exception(stringFormat(
                "Directory {} cannot be created. Check if it exists and remove it.", dir));
            // LCOV_EXCL_STOP
        }
    } catch (std::exception& e) {
        // LCOV_EXCL_START
        throw Exception(stringFormat("Failed to create directory {} due to: {}", dir, e.what()));
        // LCOV_EXCL_STOP
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
        // LCOV_EXCL_START
        throw Exception(stringFormat(
            "Error removing directory {}. Error Message: {}", dir, removeErrorCode.message()));
        // LCOV_EXCL_STOP
    }
}

void FileUtils::renameFileIfExists(const std::string& oldName, const std::string& newName) {
    // If the oldName file doesn't exist, this function does not do anything.
    if (!fileOrPathExists(oldName))
        return;
    std::error_code errorCode;
    std::filesystem::rename(oldName, newName, errorCode);
    if (errorCode.value() != 0) {
        // LCOV_EXCL_START
        throw Exception(stringFormat("Error replacing file {} to {}.  ErrorMessage: {}", oldName,
            newName, errorCode.message()));
        // LCOV_EXCL_STOP
    }
}

void FileUtils::removeFileIfExists(const std::string& path) {
    if (!fileOrPathExists(path))
        return;
    if (remove(path.c_str()) != 0) {
        // LCOV_EXCL_START
        throw Exception(stringFormat(
            "Error removing directory or file {}.  Error Message: {}", path, posixErrMessage()));
        // LCOV_EXCL_STOP
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
        throw Exception(stringFormat("Cannot set file pointer for file: {} handle: {} "
                                     "new position: {}. Error {}: {}",
            fileInfo->path, (intptr_t)fileInfo->handle, size, error,
            std::system_category().message(error)));
    }
    if (!SetEndOfFile((HANDLE)fileInfo->handle)) {
        auto error = GetLastError();
        throw Exception(stringFormat("Cannot truncate file: {} handle: {} "
                                     "size: {}. Error {}: {}",
            fileInfo->path, (intptr_t)fileInfo->handle, size, error,
            std::system_category().message(error)));
    }
#else
    if (ftruncate(fileInfo->fd, size) < 0) {
        // LCOV_EXCL_START
        throw Exception(
            stringFormat("Failed to truncate file {}: {}", fileInfo->path, posixErrMessage()));
        // LCOV_EXCL_STOP
    }
#endif
}

} // namespace common
} // namespace kuzu
