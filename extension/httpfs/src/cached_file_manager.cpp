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
    downloadBuffer = std::make_unique<uint8_t[]>(MAX_SEGMENT_SIZE);
}

CachedFileManager::~CachedFileManager() {
    auto localFileSystem = std::make_unique<LocalFileSystem>();
    localFileSystem->removeFileIfExists(cacheDir);
}

common::FileInfo* CachedFileManager::getCachedFileInfo(HTTPFileInfo* httpFileInfo) {
    std::unique_lock<std::mutex> lck{mtx};
    auto path = httpFileInfo->path;
    if (!cachedFiles.contains(path)) {
        auto fileName = FileSystem::getFileName(path);
        auto cachedFilePath = getCachedFilePath(fileName);
        auto fileInfo = vfs->openFile(cachedFilePath, O_CREAT | O_RDWR);
        downloadFile(httpFileInfo, fileInfo.get());
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

void CachedFileManager::downloadFile(HTTPFileInfo* fileToDownload, FileInfo* cacheFileInfo) {
    uint64_t numBytesRead;
    uint64_t offsetToWrite = 0;
    do {
        numBytesRead = fileToDownload->readFile(downloadBuffer.get(), MAX_SEGMENT_SIZE);
        cacheFileInfo->writeFile(downloadBuffer.get(), numBytesRead, offsetToWrite);
        offsetToWrite += numBytesRead;
    } while (numBytesRead != 0);
    fileToDownload->seek(0, SEEK_SET /* whence is unused by http filesystem seek */);
    cacheFileInfo->syncFile();
}

} // namespace httpfs
} // namespace kuzu
