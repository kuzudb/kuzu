#pragma once

#include <cstdint>
#include <string>

#include <uv.h>

extern int writeCount;
extern int returnCount;

namespace kuzu {
namespace common {

struct NodeGroupInfo {
    uint64_t totalReq = 0;
    uint64_t receivedReq = 0;
};

class FileSystem;

struct FileInfo {
#ifdef _WIN32
    FileInfo(std::string path, const void* handle, FileSystem* fileSystem)
        : path{std::move(path)}, handle{handle}, fileSystem{fileSystem} {}
#else
    FileInfo(std::string path, const int fd, FileSystem* fileSystem)
        : path{std::move(path)}, fd{fd}, fileSystem{fileSystem} {}
#endif

    ~FileInfo();

    uint64_t getFileSize();

    void readFromFile(void* buffer, uint64_t numBytes, uint64_t position);

    int64_t readFile(void* buf, size_t nbyte);

    void writeFile(const uint8_t* buffer, uint64_t numBytes, uint64_t offset);

    void writeFileAsync(const uint8_t* buffer, uint64_t numBytes, uint64_t offset, uv_loop_t* loop,
        NodeGroupInfo* info);

    int64_t seek(uint64_t offset, int whence);

    void truncate(uint64_t size);

    const std::string path;
#ifdef _WIN32
    const void* handle;
#else
    const int fd;
#endif
    FileSystem* fileSystem;
};

} // namespace common
} // namespace kuzu
