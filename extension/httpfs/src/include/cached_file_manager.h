#pragma once

#include <mutex>

#include "common/case_insensitive_map.h"
#include "common/file_system/virtual_file_system.h"

namespace kuzu {
namespace httpfs {

struct HTTPFileInfo;

struct CachedFile {
    uint64_t counter;
    std::unique_ptr<common::FileInfo> fileInfo;

    explicit CachedFile(std::unique_ptr<common::FileInfo> fileInfo)
        : counter{0}, fileInfo{std::move(fileInfo)} {}

    common::FileInfo* getFileInfo() const { return fileInfo.get(); }
};

class CachedFileManager {
public:
    static constexpr uint64_t MAX_SEGMENT_SIZE = 50000000; // 50MB

public:
    explicit CachedFileManager(main::ClientContext* context);

    ~CachedFileManager();

    common::FileInfo* getCachedFileInfo(HTTPFileInfo* httpFileInfo);

    void destroyCachedFileInfo(const std::string& path);

private:
    std::string getCachedFilePath(const std::string& originalFileName);
    void downloadFile(HTTPFileInfo* fileToDownload, common::FileInfo* cacheFileInfo);

private:
    common::case_insensitive_map_t<std::unique_ptr<CachedFile>> cachedFiles;
    common::VirtualFileSystem* vfs;
    std::string cacheDir;
    std::mutex mtx;
    std::unique_ptr<uint8_t[]> downloadBuffer;
};

} // namespace httpfs
} // namespace kuzu
