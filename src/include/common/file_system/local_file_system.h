#pragma once

#include <vector>

#include "file_system.h"

namespace kuzu {
namespace common {

struct LocalFileInfo : public FileInfo {
#ifdef _WIN32
    LocalFileInfo(std::string path, const void* handle, FileSystem* fileSystem)
        : FileInfo{std::move(path), fileSystem}, handle{handle} {}
#else
    LocalFileInfo(std::string path, const int fd, FileSystem* fileSystem)
        : FileInfo{std::move(path), fileSystem}, fd{fd} {}
#endif

    ~LocalFileInfo() override;

#ifdef _WIN32
    const void* handle;
#else
    const int fd;
#endif
};

class LocalFileSystem final : public FileSystem {
public:
    std::unique_ptr<FileInfo> openFile(const std::string& path, int flags,
        FileLockType lock_type = FileLockType::NO_LOCK) override;

    std::vector<std::string> glob(const std::string& path) override;

    void overwriteFile(const std::string& from, const std::string& to) override;

    void createDir(const std::string& dir) override;

    void removeFileIfExists(const std::string& path) override;

    bool fileOrPathExists(const std::string& path) override;

protected:
    void readFromFile(
        FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) override;

    int64_t readFile(FileInfo* fileInfo, void* buf, size_t nbyte) override;

    void writeFile(
        FileInfo* fileInfo, const uint8_t* buffer, uint64_t numBytes, uint64_t offset) override;

    int64_t seek(FileInfo* fileInfo, uint64_t offset, int whence) override;

    void truncate(FileInfo* fileInfo, uint64_t size) override;

    uint64_t getFileSize(FileInfo* fileInfo) override;
};

} // namespace common
} // namespace kuzu
