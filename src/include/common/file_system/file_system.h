#pragma once

#include <filesystem>
#include <vector>

#include "file_info.h"

namespace kuzu {
namespace common {

enum class FileLockType : uint8_t { NO_LOCK = 0, READ_LOCK = 1, WRITE_LOCK = 2 };

class FileSystem {
    friend struct FileInfo;

public:
    virtual ~FileSystem() = default;

    virtual std::unique_ptr<FileInfo> openFile(
        const std::string& path, int flags, FileLockType lock_type = FileLockType::NO_LOCK) = 0;

    virtual std::vector<std::string> glob(const std::string& path) = 0;

    virtual void overwriteFile(const std::string& from, const std::string& to) = 0;

    virtual void createDir(const std::string& dir) = 0;

    virtual void removeFileIfExists(const std::string& path) = 0;

    virtual bool fileOrPathExists(const std::string& path) = 0;

    virtual std::string joinPath(const std::string& base, const std::string& part) = 0;

    virtual std::string getFileExtension(const std::filesystem::path& path) = 0;

protected:
    virtual void readFromFile(
        FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) = 0;

    virtual int64_t readFile(FileInfo* fileInfo, void* buf, size_t nbyte) = 0;

    virtual void writeFile(
        FileInfo* fileInfo, const uint8_t* buffer, uint64_t numBytes, uint64_t offset) = 0;

    virtual int64_t seek(FileInfo* fileInfo, uint64_t offset, int whence) = 0;

    virtual void truncate(FileInfo* fileInfo, uint64_t size) = 0;

    virtual uint64_t getFileSize(FileInfo* fileInfo) = 0;
};

} // namespace common
} // namespace kuzu
