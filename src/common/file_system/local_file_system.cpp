#include "common/file_system/local_file_system.h"

#include <cstring>

#if defined(_WIN32)
#include <fileapi.h>
#include <io.h>
#include <windows.h>
#else
#include "sys/stat.h"
#include <unistd.h>
#endif

#include <fcntl.h>

#include <cstring>

#include "common/assert.h"
#include "common/exception/exception.h"
#include "common/string_format.h"
#include "common/system_message.h"
#include "glob/glob.hpp"

namespace kuzu {
namespace common {

std::unique_ptr<FileInfo> LocalFileSystem::openFile(
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
    return std::make_unique<FileInfo>(path, handle, this);
#else
    int fd = open(path.c_str(), flags, 0644);
    if (fd == -1) {
        throw Exception(stringFormat("Cannot open file {}: {}", path, posixErrMessage()));
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
    return std::make_unique<FileInfo>(path, fd, this);
#endif
}

std::vector<std::string> LocalFileSystem::glob(const std::string& path) {
    std::vector<std::string> result;
    for (auto& resultPath : glob::glob(path)) {
        result.emplace_back(resultPath.string());
    }
    return result;
}

void LocalFileSystem::overwriteFile(const std::string& from, const std::string& to) {
    if (!fileOrPathExists(from) || !fileOrPathExists(to))
        return;
    std::error_code errorCode;
    if (!std::filesystem::copy_file(
            from, to, std::filesystem::copy_options::overwrite_existing, errorCode)) {
        // LCOV_EXCL_START
        throw Exception(stringFormat(
            "Error copying file {} to {}.  ErrorMessage: {}", from, to, errorCode.message()));
        // LCOV_EXCL_STOP
    }
}

void LocalFileSystem::createDir(const std::string& dir) {
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

void LocalFileSystem::removeFileIfExists(const std::string& path) {
    if (!fileOrPathExists(path))
        return;
    if (remove(path.c_str()) != 0) {
        // LCOV_EXCL_START
        throw Exception(stringFormat(
            "Error removing directory or file {}.  Error Message: {}", path, posixErrMessage()));
        // LCOV_EXCL_STOP
    }
}

bool LocalFileSystem::fileOrPathExists(const std::string& path) {
    return std::filesystem::exists(path);
}

std::string LocalFileSystem::joinPath(const std::string& base, const std::string& part) {
    return (std::filesystem::path(base) / part).string();
}

std::string LocalFileSystem::getFileExtension(const std::filesystem::path& path) {
    return path.extension().string();
}

void LocalFileSystem::readFromFile(
    FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) {
#if defined(_WIN32)
    DWORD numBytesRead;
    OVERLAPPED overlapped{0, 0, 0, 0};
    overlapped.Offset = position & 0xffffffff;
    overlapped.OffsetHigh = position >> 32;
    if (!ReadFile((HANDLE)fileInfo->handle, buffer, numBytes, &numBytesRead, &overlapped)) {
        auto error = GetLastError();
        throw Exception(
            stringFormat("Cannot read from file: {} handle: {} "
                         "numBytesRead: {} numBytesToRead: {} position: {}. Error {}: {}",
                fileInfo->path, (intptr_t)fileInfo->handle, numBytesRead, numBytes, position, error,
                std::system_category().message(error)));
    }
    if (numBytesRead != numBytes && fileInfo->getFileSize() != position + numBytesRead) {
        throw Exception(stringFormat("Cannot read from file: {} handle: {} "
                                     "numBytesRead: {} numBytesToRead: {} position: {}",
            fileInfo->path, (intptr_t)fileInfo->handle, numBytesRead, numBytes, position));
    }
#else
    auto numBytesRead = pread(fileInfo->fd, buffer, numBytes, position);
    if ((uint64_t)numBytesRead != numBytes && fileInfo->getFileSize() != position + numBytesRead) {
        // LCOV_EXCL_START
        throw Exception(stringFormat("Cannot read from file: {} fileDescriptor: {} "
                                     "numBytesRead: {} numBytesToRead: {} position: {}",
            fileInfo->path, fileInfo->fd, numBytesRead, numBytes, position));
        // LCOV_EXCL_STOP
    }
#endif
}

int64_t LocalFileSystem::readFile(FileInfo* fileInfo, void* buf, size_t nbyte) {
#if defined(_WIN32)
    DWORD numBytesRead;
    ReadFile((HANDLE)fileInfo->handle, buf, nbyte, &numBytesRead, nullptr);
    return numBytesRead;
#else
    return read(fileInfo->fd, buf, nbyte);
#endif
}

void LocalFileSystem::writeFile(
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

int64_t LocalFileSystem::seek(FileInfo* fileInfo, uint64_t offset, int whence) {
#if defined(_WIN32)
    LARGE_INTEGER result;
    LARGE_INTEGER offset_;
    offset_.QuadPart = offset;
    SetFilePointerEx((HANDLE)fileInfo->handle, offset_, &result, whence);
    return result.QuadPart;
#else
    return lseek(fileInfo->fd, offset, whence);
#endif
}

void LocalFileSystem::truncate(FileInfo* fileInfo, uint64_t size) {
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

uint64_t LocalFileSystem::getFileSize(kuzu::common::FileInfo* fileInfo) {
#ifdef _WIN32
    LARGE_INTEGER size;
    if (!GetFileSizeEx((HANDLE)fileInfo->handle, &size)) {
        auto error = GetLastError();
        throw Exception(stringFormat("Cannot read size of file. path: {} - Error {}: {}",
            fileInfo->path, error, systemErrMessage(error)));
    }
    return size.QuadPart;
#else
    struct stat s;
    if (fstat(fileInfo->fd, &s) == -1) {
        throw Exception(stringFormat("Cannot read size of file. path: {} - Error {}: {}",
            fileInfo->path, errno, posixErrMessage()));
    }
    KU_ASSERT(s.st_size >= 0);
    return s.st_size;
#endif
}

} // namespace common
} // namespace kuzu
