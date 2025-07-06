#include "cached_file_manager.h"

#include "common/string_utils.h"
#include "httpfs.h"
#include "httpfs_extension.h"

namespace kuzu {
namespace httpfs_extension {

using namespace common;

CachedFileManager::CachedFileManager(main::ClientContext* context) : vfs{context->getVFSUnsafe()} {
    cacheDir = common::stringFormat("{}/{}",
        extension::ExtensionUtils::getLocalDirForExtension(context,
            StringUtils::getLower(HttpfsExtension::EXTENSION_NAME)),
        ".cached_files");
    if (!vfs->fileOrPathExists(cacheDir, context)) {
        vfs->createDir(cacheDir);
    }
    downloadBuffer = std::make_unique<uint8_t[]>(MAX_SEGMENT_SIZE);
}

std::unique_ptr<FileInfo> CachedFileManager::getCachedFileInfo(HTTPFileInfo* httpFileInfo,
    common::transaction_t transactionID) {
    std::unique_lock<std::mutex> lck{mtx};
    auto cachePathForTrx = getCachedDirForTrx(transactionID);
    if (!vfs->fileOrPathExists(cachePathForTrx)) {
        vfs->createDir(cachePathForTrx);
    }
    auto fileName = FileSystem::getFileName(httpFileInfo->path);
    auto cacheFilePath = getCachedFilePath(fileName, transactionID);
    if (!vfs->fileOrPathExists(cacheFilePath)) {
        auto cacheFileInfo =
            vfs->openFile(cacheFilePath, FileOpenFlags(FileFlags::CREATE_IF_NOT_EXISTS |
                                                       FileFlags::READ_ONLY | FileFlags::WRITE));
        downloadFile(httpFileInfo, cacheFileInfo.get());
    }
    return vfs->openFile(cacheFilePath, FileOpenFlags(FileFlags::READ_ONLY));
}

void CachedFileManager::cleanUP(main::ClientContext* context) {
    auto cacheDirForTrx = getCachedDirForTrx(context->getTransaction()->getID());
    vfs->removeFileIfExists(cacheDirForTrx, context);
}

std::string CachedFileManager::getCachedFilePath(const std::string& originalFileName,
    common::transaction_t transactionID) {
    return common::stringFormat("{}/{}/{}", cacheDir, transactionID, originalFileName);
}

std::string CachedFileManager::getCachedDirForTrx(common::transaction_t transactionID) {
    return common::stringFormat("{}/{}", cacheDir, transactionID);
}

void CachedFileManager::downloadFile(HTTPFileInfo* fileToDownload, FileInfo* cacheFileInfo) {
    fileToDownload->initMetadata();
    uint64_t numBytesRead = 0;
    uint64_t offsetToWrite = 0;
    do {
        numBytesRead = fileToDownload->readFile(downloadBuffer.get(), MAX_SEGMENT_SIZE);
        cacheFileInfo->writeFile(downloadBuffer.get(), numBytesRead, offsetToWrite);
        offsetToWrite += numBytesRead;
    } while (numBytesRead != 0);
    fileToDownload->seek(0, SEEK_SET /* whence is unused by http filesystem seek */);
    cacheFileInfo->syncFile();
}

} // namespace httpfs_extension
} // namespace kuzu
