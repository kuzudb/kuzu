#include "storage/wal/shadow_file.h"

#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/file_handle.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace storage {

void ShadowPageRecord::serialize(Serializer& serializer) const {
    serializer.write<DBFileID>(dbFileID);
    serializer.write<file_idx_t>(originalFileIdx);
    serializer.write<page_idx_t>(originalPageIdx);
}

ShadowPageRecord ShadowPageRecord::deserialize(Deserializer& deserializer) {
    DBFileID dbFileID;
    file_idx_t originalFileIdx = INVALID_FILE_IDX;
    page_idx_t originalPageIdx = INVALID_PAGE_IDX;
    deserializer.deserializeValue<DBFileID>(dbFileID);
    deserializer.deserializeValue<file_idx_t>(originalFileIdx);
    deserializer.deserializeValue<page_idx_t>(originalPageIdx);
    return ShadowPageRecord{dbFileID, originalFileIdx, originalPageIdx};
}

ShadowFile::ShadowFile(const std::string& directory, bool readOnly, BufferManager& bufferManager,
    VirtualFileSystem* vfs, ClientContext* context) {
    if (DBConfig::isDBPathInMemory(directory)) {
        return;
    }
    shadowingFH = bufferManager.getFileHandle(
        vfs->joinPath(directory, std::string(StorageConstants::SHADOWING_SUFFIX)),
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        vfs, context);
    if (shadowingFH->getNumPages() == 0) {
        // Reserve the first page for the header.
        shadowingFH->addNewPage();
    }
}

void ShadowFile::clearShadowPage(file_idx_t originalFile, page_idx_t originalPage) {
    if (hasShadowPage(originalFile, originalPage)) {
        shadowPagesMap.at(originalFile).erase(originalPage);
        if (shadowPagesMap.at(originalFile).empty()) {
            shadowPagesMap.erase(originalFile);
        }
    }
}

page_idx_t ShadowFile::getOrCreateShadowPage(DBFileID dbFileID, file_idx_t originalFile,
    page_idx_t originalPage) {
    if (hasShadowPage(originalFile, originalPage)) {
        return shadowPagesMap[originalFile][originalPage];
    }
    const auto shadowPageIdx = shadowingFH->addNewPage();
    shadowPagesMap[originalFile][originalPage] = shadowPageIdx;
    shadowPageRecords.push_back({dbFileID, originalFile, originalPage});
    return shadowPageIdx;
}

page_idx_t ShadowFile::getShadowPage(file_idx_t originalFile, page_idx_t originalPage) const {
    KU_ASSERT(hasShadowPage(originalFile, originalPage));
    return shadowPagesMap.at(originalFile).at(originalPage);
}

void ShadowFile::replayShadowPageRecords(ClientContext& context) const {
    std::unordered_map<DBFileID, std::unique_ptr<FileInfo>> fileCache;
    const auto pageBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    page_idx_t shadowPageIdx = 1; // Skip header page.
    for (const auto& record : shadowPageRecords) {
        const auto& dbFileID = record.dbFileID;
        if (!fileCache.contains(dbFileID)) {
            fileCache.insert(std::make_pair(dbFileID, getFileInfo(context, dbFileID)));
        }
        const auto& fileInfoOfDBFile = fileCache.at(dbFileID);
        shadowingFH->readPageFromDisk(pageBuffer.get(), shadowPageIdx++);
        fileInfoOfDBFile->writeFile(pageBuffer.get(), KUZU_PAGE_SIZE,
            record.originalPageIdx * KUZU_PAGE_SIZE);
        // NOTE: We're not taking lock here, as we assume this is only called with single thread.
        context.getMemoryManager()->getBufferManager()->updateFrameIfPageIsInFrameWithoutLock(
            record.originalFileIdx, pageBuffer.get(), record.originalPageIdx);
    }
}

void ShadowFile::flushAll() const {
    // Write header page to file.
    ShadowFileHeader header;
    header.numShadowPages = shadowPageRecords.size();
    const auto headerBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    memcpy(headerBuffer.get(), &header, sizeof(ShadowFileHeader));
    KU_ASSERT(!shadowingFH->isInMemoryMode());
    shadowingFH->writePageToFile(headerBuffer.get(), 0);
    // Flush shadow pages to file.
    shadowingFH->flushAllDirtyPagesInFrames();
    // Append shadow page records to end of file.
    const auto writer = std::make_shared<BufferedFileWriter>(*shadowingFH->getFileInfo());
    writer->setFileOffset(shadowingFH->getNumPages() * KUZU_PAGE_SIZE);
    Serializer ser(writer);
    KU_ASSERT(shadowPageRecords.size() + 1 == shadowingFH->getNumPages());
    ser.serializeVector(shadowPageRecords);
    writer->flush();
    // Sync the file to disk.
    writer->sync();
}

void ShadowFile::clearAll(ClientContext& context) {
    context.getMemoryManager()->getBufferManager()->removeFilePagesFromFrames(*shadowingFH);
    shadowingFH->resetToZeroPagesAndPageCapacity();
    shadowPagesMap.clear();
    shadowPageRecords.clear();
    // Reserve header page.
    shadowingFH->addNewPage();
}

std::unique_ptr<FileInfo> ShadowFile::getFileInfo(const ClientContext& context, DBFileID dbFileID) {
    std::string fileName;
    switch (dbFileID.dbFileType) {
    case DBFileType::DATA: {
        fileName = StorageUtils::getDataFName(context.getVFSUnsafe(), context.getDatabasePath());
    } break;
    case DBFileType::NODE_INDEX: {
        fileName = StorageUtils::getNodeIndexFName(context.getVFSUnsafe(),
            context.getDatabasePath(), dbFileID.tableID, FileVersionType::ORIGINAL);
        if (dbFileID.isOverflow) {
            fileName = StorageUtils::getOverflowFileName(fileName);
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return context.getVFSUnsafe()->openFile(fileName, FileFlags::READ_ONLY | FileFlags::WRITE);
}

} // namespace storage
} // namespace kuzu
