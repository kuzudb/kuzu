#include "cached_file_manager.h"

#include "httpfs.h"

namespace kuzu {
namespace httpfs {

using namespace common;

CachedFileManager::CachedFileManager(main::ClientContext* context) : vfs{context->getVFSUnsafe()} {
    cacheDir = context->getDatabasePath() + "/.cached_files";
    if (!vfs->fileOrPathExists(cacheDir, context)) {
        vfs->createDir(cacheDir);
    }
}

CachedFileManager::~CachedFileManager() {
    auto localFileSystem = std::make_unique<LocalFileSystem>();
    localFileSystem->removeFileIfExists(cacheDir);
}

common::FileInfo* CachedFileManager::getCachedFileInfo(const std::string& path) {
    std::unique_lock<std::mutex> lck{mtx};
    if (!cachedFiles.contains(path)) {
        auto fileName = FileSystem::getFileName(path);
        auto cachedFilePath = getCachedFilePath(fileName);
        auto fileInfo = vfs->openFile(cachedFilePath, O_CREAT | O_RDWR);
        downloadFile(path, fileInfo.get());
        cachedFiles.emplace(path, std::make_unique<CachedFile>(std::move(fileInfo)));
    }
    cachedFiles.at(path)->counter++;
    return cachedFiles.at(path)->getFileInfo();
}

void CachedFileManager::destroyCachedFileInfo(const std::string& path) {
    std::unique_lock<std::mutex> lck{mtx};
    KU_ASSERT(cachedFiles.contains(path));
    cachedFiles.at(path)->counter--;
    if (cachedFiles.at(path)->counter == 0) {
        cachedFiles.erase(path);
    }
}

std::string CachedFileManager::getCachedFilePath(const std::string& originalFileName) {
    return common::stringFormat("{}/{}-{}", cacheDir, originalFileName, std::time(nullptr));
}

void CachedFileManager::downloadFile(const std::string& path, FileInfo* info) {
    auto url = HTTPFileSystem::parseUrl(path);
    httplib::Client cli(url.first);
    httplib::Headers headers = {{}};
    auto fileContent = cli.Get(url.second, headers)->body;
    info->writeFile(reinterpret_cast<const uint8_t*>(fileContent.c_str()), fileContent.size(),
        0 /* offset */);
}

} // namespace httpfs
} // namespace kuzu
