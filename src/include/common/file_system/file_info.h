#pragma once

#include <cstdint>
#include <string>

#include "common/api.h"
#include "common/cast.h"

namespace kuzu {
namespace common {

class FileSystem;

struct KUZU_API FileInfo {
    FileInfo(std::string path, FileSystem* fileSystem)
        : path{std::move(path)}, fileSystem{fileSystem} {}

    virtual ~FileInfo() = default;

    uint64_t getFileSize() const;

    void readFromFile(void* buffer, uint64_t numBytes, uint64_t position);

    int64_t readFile(void* buf, size_t nbyte);

    void writeFile(const uint8_t* buffer, uint64_t numBytes, uint64_t offset);

    void syncFile() const;

    int64_t seek(uint64_t offset, int whence);

    void truncate(uint64_t size);

    template<class TARGET>
    TARGET* ptrCast() {
        return common::ku_dynamic_cast<FileInfo*, TARGET*>(this);
    }

    template<class TARGET>
    const TARGET* constPtrCast() const {
        return common::ku_dynamic_cast<const FileInfo*, const TARGET*>(this);
    }

    const std::string path;

    FileSystem* fileSystem;
};

} // namespace common
} // namespace kuzu
