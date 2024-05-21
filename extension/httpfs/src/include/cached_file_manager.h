#pragma once

#include <mutex>

#include "common/case_insensitive_map.h"
#include "common/file_system/virtual_file_system.h"

namespace kuzu {
namespace httpfs {

struct CachedFile {
    uint64_t counter;
    std::unique_ptr<common::FileInfo> fileInfo;

    explicit CachedFile(std::unique_ptr<common::FileInfo> fileInfo)
        : counter{0}, fileInfo{std::move(fileInfo)} {}

    common::FileInfo* getFileInfo() const { return fileInfo.get(); }
};

class CachedFileManager {
public:
    explicit CachedFileManager(main::ClientContext* context);

    ~CachedFileManager();

    common::FileInfo* getCachedFileInfo(const std::string& path);

    void destroyCachedFileInfo(const std::string& path);

private:
    std::string getCachedFilePath(const std::string& originalFileName);
    void downloadFile(const std::string& path, common::FileInfo* info);

private:
    common::case_insensitive_map_t<std::unique_ptr<CachedFile>> cachedFiles;
    common::VirtualFileSystem* vfs;
    std::string cacheDir;
    std::mutex mtx;
};

} // namespace httpfs
} // namespace kuzu
