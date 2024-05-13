#include "common/file_system/local_file_system.h"

#include "common/assert.h"
#include "common/exception/io.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "common/system_message.h"
#include "glob/glob.hpp"
#include "main/client_context.h"
#include "main/settings.h"

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

namespace kuzu {
namespace common {

LocalFileInfo::~LocalFileInfo() {
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

std::unique_ptr<FileInfo> LocalFileSystem::openFile(const std::string& path, int flags,
    main::ClientContext* context, FileLockType lock_type) {
    auto fullPath = expandPath(context, path);
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

    HANDLE handle = CreateFileA(fullPath.c_str(), dwDesiredAccess, dwShareMode, nullptr,
        dwCreationDisposition, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (handle == INVALID_HANDLE_VALUE) {
        throw IOException(stringFormat("Cannot open file. path: {} - Error {}: {}", fullPath,
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
            throw IOException("Could not set lock on file : " + fullPath);
        }
    }
    return std::make_unique<LocalFileInfo>(fullPath, handle, this);
#else
    int fd = open(fullPath.c_str(), flags, 0644);
    if (fd == -1) {
        throw IOException(stringFormat("Cannot open file {}: {}", fullPath, posixErrMessage()));
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
            throw IOException("Could not set lock on file : " + fullPath);
        }
    }
    return std::make_unique<LocalFileInfo>(fullPath, fd, this);
#endif
}

std::vector<std::string> LocalFileSystem::glob(main::ClientContext* context,
    const std::string& path) const {
    if (path.empty()) {
        return std::vector<std::string>();
    }
    std::vector<std::string> pathsToGlob;
    if (path[0] == '/' || (std::isalpha(path[0]) && path[1] == ':')) {
        // Note:
        // Unix absolute path starts with '/'
        // Windows absolute path starts with "[DiskID]://"
        pathsToGlob.push_back(path);
    } else if (path[0] == '~') {
        // Expands home directory
        auto homeDirectory =
            context->getCurrentSetting(main::HomeDirectorySetting::name).getValue<std::string>();
        pathsToGlob.push_back(homeDirectory + path.substr(1));
    } else {
        // Relative path to the file search path.
        auto fileSearchPath =
            context->getCurrentSetting(main::FileSearchPathSetting::name).getValue<std::string>();
        auto searchPaths = common::StringUtils::split(fileSearchPath, ",");
        for (auto& searchPath : searchPaths) {
            pathsToGlob.push_back(common::stringFormat("{}/{}", searchPath, path));
        }
        // Relative path to the current directory.
        pathsToGlob.push_back(path);
    }
    std::vector<std::string> result;
    for (auto& pathToGlob : pathsToGlob) {
        for (auto& resultPath : glob::glob(pathToGlob)) {
            result.emplace_back(resultPath.string());
        }
    }
    return result;
}

void LocalFileSystem::overwriteFile(const std::string& from, const std::string& to) {
    if (!fileOrPathExists(from) || !fileOrPathExists(to))
        return;
    std::error_code errorCode;
    if (!std::filesystem::copy_file(from, to, std::filesystem::copy_options::overwrite_existing,
            errorCode)) {
        // LCOV_EXCL_START
        throw IOException(stringFormat("Error copying file {} to {}.  ErrorMessage: {}", from, to,
            errorCode.message()));
        // LCOV_EXCL_STOP
    }
}

void LocalFileSystem::copyFile(const std::string& from, const std::string& to) {
    if (!fileOrPathExists(from))
        return;
    std::error_code errorCode;
    if (!std::filesystem::copy_file(from, to, std::filesystem::copy_options::none, errorCode)) {
        // LCOV_EXCL_START
        throw IOException(stringFormat("Error copying file {} to {}.  ErrorMessage: {}", from, to,
            errorCode.message()));
        // LCOV_EXCL_STOP
    }
}

void LocalFileSystem::createDir(const std::string& dir) const {
    try {
        if (std::filesystem::exists(dir)) {
            // LCOV_EXCL_START
            throw IOException(stringFormat("Directory {} already exists.", dir));
            // LCOV_EXCL_STOP
        }
        auto directoryToCreate = dir;
        if (directoryToCreate.ends_with('/')) {
            // This is a known issue with std::filesystem::create_directories. (link:
            // https://github.com/llvm/llvm-project/issues/60634). We have to manually remove the
            // last '/' if the path ends with '/'.
            directoryToCreate = directoryToCreate.substr(0, directoryToCreate.size() - 1);
        }
        std::error_code errCode;
        if (!std::filesystem::create_directories(directoryToCreate, errCode)) {
            // LCOV_EXCL_START
            throw IOException(
                stringFormat("Directory {} cannot be created. Check if it exists and remove it.",
                    directoryToCreate));
            // LCOV_EXCL_STOP
        }
        if (errCode) {
            // LCOV_EXCL_START
            throw IOException(stringFormat("Failed to create directory: {}, error message: {}.",
                dir, errCode.message()));
            // LCOV_EXCL_STOP
        }
    } catch (std::exception& e) {
        // LCOV_EXCL_START
        throw IOException(stringFormat("Failed to create directory {} due to: {}", dir, e.what()));
        // LCOV_EXCL_STOP
    }
}

void LocalFileSystem::removeFileIfExists(const std::string& path) {
    if (!fileOrPathExists(path))
        return;
    if (remove(path.c_str()) != 0) {
        // LCOV_EXCL_START
        throw IOException(stringFormat("Error removing directory or file {}.  Error Message: {}",
            path, posixErrMessage()));
        // LCOV_EXCL_STOP
    }
}

bool LocalFileSystem::fileOrPathExists(const std::string& path) {
    return std::filesystem::exists(path);
}

std::string LocalFileSystem::expandPath(main::ClientContext* context,
    const std::string& path) const {
    auto fullPath = path;
    if (path.starts_with('~')) {
        fullPath =
            context->getCurrentSetting(main::HomeDirectorySetting::name).getValue<std::string>() +
            fullPath.substr(1);
    }
    return fullPath;
}

void LocalFileSystem::readFromFile(FileInfo& fileInfo, void* buffer, uint64_t numBytes,
    uint64_t position) const {
    auto localFileInfo = fileInfo.constPtrCast<LocalFileInfo>();
#if defined(_WIN32)
    DWORD numBytesRead;
    OVERLAPPED overlapped{0, 0, 0, 0};
    overlapped.Offset = position & 0xffffffff;
    overlapped.OffsetHigh = position >> 32;
    if (!ReadFile((HANDLE)localFileInfo->handle, buffer, numBytes, &numBytesRead, &overlapped)) {
        auto error = GetLastError();
        throw IOException(
            stringFormat("Cannot read from file: {} handle: {} "
                         "numBytesRead: {} numBytesToRead: {} position: {}. Error {}: {}",
                fileInfo.path, (intptr_t)localFileInfo->handle, numBytesRead, numBytes, position,
                error, std::system_category().message(error)));
    }
    if (numBytesRead != numBytes && fileInfo.getFileSize() != position + numBytesRead) {
        throw IOException(stringFormat("Cannot read from file: {} handle: {} "
                                       "numBytesRead: {} numBytesToRead: {} position: {}",
            fileInfo.path, (intptr_t)localFileInfo->handle, numBytesRead, numBytes, position));
    }
#else
    auto numBytesRead = pread(localFileInfo->fd, buffer, numBytes, position);
    if ((uint64_t)numBytesRead != numBytes &&
        localFileInfo->getFileSize() != position + numBytesRead) {
        // LCOV_EXCL_START
        throw IOException(stringFormat("Cannot read from file: {} fileDescriptor: {} "
                                       "numBytesRead: {} numBytesToRead: {} position: {}",
            fileInfo.path, localFileInfo->fd, numBytesRead, numBytes, position));
        // LCOV_EXCL_STOP
    }
#endif
}

int64_t LocalFileSystem::readFile(FileInfo& fileInfo, void* buf, size_t nbyte) const {
    auto localFileInfo = fileInfo.constPtrCast<LocalFileInfo>();
#if defined(_WIN32)
    DWORD numBytesRead;
    ReadFile((HANDLE)localFileInfo->handle, buf, nbyte, &numBytesRead, nullptr);
    return numBytesRead;
#else
    return read(localFileInfo->fd, buf, nbyte);
#endif
}

void LocalFileSystem::writeFile(FileInfo& fileInfo, const uint8_t* buffer, uint64_t numBytes,
    uint64_t offset) const {
    auto localFileInfo = fileInfo.constPtrCast<LocalFileInfo>();
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
        if (!WriteFile((HANDLE)localFileInfo->handle, buffer + bufferOffset, numBytesToWrite,
                &numBytesWritten, &overlapped)) {
            auto error = GetLastError();
            throw IOException(
                stringFormat("Cannot write to file. path: {} handle: {} offsetToWrite: {} "
                             "numBytesToWrite: {} numBytesWritten: {}. Error {}: {}.",
                    fileInfo.path, (intptr_t)localFileInfo->handle, offset, numBytesToWrite,
                    numBytesWritten, error, std::system_category().message(error)));
        }
#else
        auto numBytesWritten =
            pwrite(localFileInfo->fd, buffer + bufferOffset, numBytesToWrite, offset);
        if (numBytesWritten != (int64_t)numBytesToWrite) {
            // LCOV_EXCL_START
            throw IOException(
                stringFormat("Cannot write to file. path: {} fileDescriptor: {} offsetToWrite: {} "
                             "numBytesToWrite: {} numBytesWritten: {}. Error: {}",
                    fileInfo.path, localFileInfo->fd, offset, numBytesToWrite, numBytesWritten,
                    posixErrMessage()));
            // LCOV_EXCL_STOP
        }
#endif
        remainingNumBytesToWrite -= numBytesWritten;
        offset += numBytesWritten;
        bufferOffset += numBytesWritten;
    }
}

void LocalFileSystem::syncFile(const FileInfo& fileInfo) const {
    auto localFileInfo = fileInfo.constPtrCast<LocalFileInfo>();
#if defined(_WIN32)
    // Note that `FlushFileBuffers` returns 0 when fail, while `fsync` returns 0 when succeed.
    if (FlushFileBuffers((HANDLE)localFileInfo->handle) == 0) {
        auto error = GetLastError();
        throw IOException(stringFormat("Failed to sync file {}. Error {}: {}", fileInfo.path, error,
            std::system_category().message(error)));
    }
#else
    if (fsync(localFileInfo->fd) != 0) {
        throw IOException(stringFormat("Failed to sync file {}.", fileInfo.path));
    }
#endif
}

int64_t LocalFileSystem::seek(FileInfo& fileInfo, uint64_t offset, int whence) const {
    auto localFileInfo = fileInfo.constPtrCast<LocalFileInfo>();
#if defined(_WIN32)
    LARGE_INTEGER result;
    LARGE_INTEGER offset_;
    offset_.QuadPart = offset;
    SetFilePointerEx((HANDLE)localFileInfo->handle, offset_, &result, whence);
    return result.QuadPart;
#else
    return lseek(localFileInfo->fd, offset, whence);
#endif
}

void LocalFileSystem::truncate(FileInfo& fileInfo, uint64_t size) const {
    auto localFileInfo = fileInfo.constPtrCast<LocalFileInfo>();
#if defined(_WIN32)
    auto offsetHigh = (LONG)(size >> 32);
    LONG* offsetHighPtr = NULL;
    if (offsetHigh > 0)
        offsetHighPtr = &offsetHigh;
    if (SetFilePointer((HANDLE)localFileInfo->handle, size & 0xffffffff, offsetHighPtr,
            FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
        auto error = GetLastError();
        throw IOException(stringFormat("Cannot set file pointer for file: {} handle: {} "
                                       "new position: {}. Error {}: {}",
            fileInfo.path, (intptr_t)localFileInfo->handle, size, error,
            std::system_category().message(error)));
    }
    if (!SetEndOfFile((HANDLE)localFileInfo->handle)) {
        auto error = GetLastError();
        throw IOException(stringFormat("Cannot truncate file: {} handle: {} "
                                       "size: {}. Error {}: {}",
            fileInfo.path, (intptr_t)localFileInfo->handle, size, error,
            std::system_category().message(error)));
    }
#else
    if (ftruncate(localFileInfo->fd, size) < 0) {
        // LCOV_EXCL_START
        throw IOException(
            stringFormat("Failed to truncate file {}: {}", fileInfo.path, posixErrMessage()));
        // LCOV_EXCL_STOP
    }
#endif
}

uint64_t LocalFileSystem::getFileSize(const FileInfo& fileInfo) const {
    auto localFileInfo = fileInfo.constPtrCast<LocalFileInfo>();
#ifdef _WIN32
    LARGE_INTEGER size;
    if (!GetFileSizeEx((HANDLE)localFileInfo->handle, &size)) {
        auto error = GetLastError();
        throw IOException(stringFormat("Cannot read size of file. path: {} - Error {}: {}",
            fileInfo.path, error, systemErrMessage(error)));
    }
    return size.QuadPart;
#else
    struct stat s;
    if (fstat(localFileInfo->fd, &s) == -1) {
        throw IOException(stringFormat("Cannot read size of file. path: {} - Error {}: {}",
            fileInfo.path, errno, posixErrMessage()));
    }
    KU_ASSERT(s.st_size >= 0);
    return s.st_size;
#endif
}

} // namespace common
} // namespace kuzu
