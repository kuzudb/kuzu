#include "storage/wal/shadow_file.h"

#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "main/client_context.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/storage_utils.h"
#include <sys/fcntl.h>

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace storage {

DBFileID DBFileID::newDataFileID() {
    DBFileID retVal;
    retVal.dbFileType = DBFileType::DATA;
    return retVal;
}

DBFileID DBFileID::newPKIndexFileID(table_id_t tableID) {
    DBFileID retVal;
    retVal.dbFileType = DBFileType::NODE_INDEX;
    retVal.tableID = tableID;
    return retVal;
}

void ShadowPageRecord::serialize(Serializer& serializer) const {
    serializer.write<DBFileID>(dbFileID);
    serializer.write<file_idx_t>(originalFileIdx);
    serializer.write<page_idx_t>(originalPageIdx);
}

ShadowPageRecord ShadowPageRecord::deserialize(Deserializer& deserializer) {
    DBFileID dbFileID;
    file_idx_t originalFileIdx;
    page_idx_t originalPageIdx;
    deserializer.deserializeValue<DBFileID>(dbFileID);
    deserializer.deserializeValue<file_idx_t>(originalFileIdx);
    deserializer.deserializeValue<page_idx_t>(originalPageIdx);
    return ShadowPageRecord{dbFileID, originalFileIdx, originalPageIdx};
}

ShadowFile::ShadowFile(const std::string& directory, bool readOnly, BufferManager& bufferManager,
    VirtualFileSystem* vfs, ClientContext* context) {
    shadowingFH = bufferManager.getBMFileHandle(
        vfs->joinPath(directory, std::string(StorageConstants::SHADOWING_SUFFIX)),
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        vfs, context);
    // Reserve the first page for the header.
    shadowingFH->addNewPage();
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
    const auto pageBuffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
    page_idx_t shadowPageIdx = 1; // Skip header page.
    for (const auto& record : shadowPageRecords) {
        const auto& dbFileID = record.dbFileID;
        if (!fileCache.contains(dbFileID)) {
            fileCache.insert(std::make_pair(dbFileID, getFileInfo(context, dbFileID)));
        }
        const auto& fileInfoOfDBFile = fileCache.at(dbFileID);
        shadowingFH->readPage(pageBuffer.get(), shadowPageIdx++);
        fileInfoOfDBFile->writeFile(pageBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE,
            record.originalPageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
        // NOTE: We're not taking lock here, as we assume this is only called with single thread.
        context.getMemoryManager()->getBufferManager()->updateFrameIfPageIsInFrameWithoutLock(
            record.originalFileIdx, pageBuffer.get(), record.originalPageIdx);
    }
}

void ShadowFile::flushAll(ClientContext& context) const {
    // Write header page to file.
    ShadowFileHeader header;
    header.numShadowPages = shadowPageRecords.size();
    const auto headerBuffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
    memcpy(headerBuffer.get(), &header, sizeof(ShadowFileHeader));
    shadowingFH->writePage(headerBuffer.get(), 0);
    // Flush shadow pages to file.
    context.getMemoryManager()->getBufferManager()->flushAllDirtyPagesInFrames(*shadowingFH);
    // Append shadow page records to end of file.
    const auto writer = std::make_shared<BufferedFileWriter>(*shadowingFH->getFileInfo());
    writer->setFileOffset(shadowingFH->getNumPages() * BufferPoolConstants::PAGE_4KB_SIZE);
    Serializer ser(writer);
    KU_ASSERT(shadowPageRecords.size() + 1 == shadowingFH->getNumPages());
    ser.serializeVector(shadowPageRecords);
    writer->flush();
    // Sync the file to disk.
    shadowingFH->getFileInfo()->syncFile();
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
    return context.getVFSUnsafe()->openFile(fileName, O_RDWR);
}

} // namespace storage
} // namespace kuzu