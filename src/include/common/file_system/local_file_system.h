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
        main::ClientContext* context = nullptr,
        FileLockType lock_type = FileLockType::NO_LOCK) override;

    std::vector<std::string> glob(
        main::ClientContext* context, const std::string& path) const override;

    void overwriteFile(const std::string& from, const std::string& to) const override;

    void copyFile(const std::string& from, const std::string& to) const override;

    void createDir(const std::string& dir) const override;

    void removeFileIfExists(const std::string& path) const override;

    bool fileOrPathExists(const std::string& path) const override;

protected:
    void readFromFile(
        FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) const override;

    int64_t readFile(FileInfo* fileInfo, void* buf, size_t nbyte) const override;

    void writeFile(FileInfo* fileInfo, const uint8_t* buffer, uint64_t numBytes,
        uint64_t offset) const override;

    int64_t seek(FileInfo* fileInfo, uint64_t offset, int whence) const override;

    void truncate(FileInfo* fileInfo, uint64_t size) const override;

    uint64_t getFileSize(FileInfo* fileInfo) const override;
};

} // namespace common
} // namespace kuzu
