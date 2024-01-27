#pragma once

#include <filesystem>
#include <vector>

#include "common/assert.h"
#include "file_info.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main

namespace common {

enum class FileLockType : uint8_t { NO_LOCK = 0, READ_LOCK = 1, WRITE_LOCK = 2 };

class KUZU_API FileSystem {
    friend struct FileInfo;

public:
    virtual ~FileSystem() = default;

    virtual std::unique_ptr<FileInfo> openFile(const std::string& path, int flags,
        main::ClientContext* context = nullptr, FileLockType lock_type = FileLockType::NO_LOCK) = 0;

    virtual std::vector<std::string> glob(
        main::ClientContext* context, const std::string& path) const = 0;

    virtual void overwriteFile(const std::string& from, const std::string& to) const;

    virtual void createDir(const std::string& dir) const;

    virtual void removeFileIfExists(const std::string& path) const;

    virtual bool fileOrPathExists(const std::string& path) const;

    static std::string joinPath(const std::string& base, const std::string& part);

    static std::string getFileExtension(const std::filesystem::path& path);

    virtual bool canHandleFile(const std::string& /*path*/) const { KU_UNREACHABLE; }

protected:
    virtual void readFromFile(
        FileInfo* fileInfo, void* buffer, uint64_t numBytes, uint64_t position) const = 0;

    virtual int64_t readFile(FileInfo* fileInfo, void* buf, size_t nbyte) const = 0;

    virtual void writeFile(
        FileInfo* fileInfo, const uint8_t* buffer, uint64_t numBytes, uint64_t offset) const;

    virtual int64_t seek(FileInfo* fileInfo, uint64_t offset, int whence) const = 0;

    virtual void truncate(FileInfo* fileInfo, uint64_t size) const;

    virtual uint64_t getFileSize(FileInfo* fileInfo) const = 0;
};

} // namespace common
} // namespace kuzu
