#include "storage/shadow_file.h"

#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "main/client_context.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/file_handle.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace storage {

void ShadowPageRecord::serialize(Serializer& serializer) const {
    serializer.write<file_idx_t>(originalFileIdx);
    serializer.write<page_idx_t>(originalPageIdx);
}

ShadowPageRecord ShadowPageRecord::deserialize(Deserializer& deserializer) {
    file_idx_t originalFileIdx = INVALID_FILE_IDX;
    page_idx_t originalPageIdx = INVALID_PAGE_IDX;
    deserializer.deserializeValue<file_idx_t>(originalFileIdx);
    deserializer.deserializeValue<page_idx_t>(originalPageIdx);
    return ShadowPageRecord{originalFileIdx, originalPageIdx};
}

ShadowFile::ShadowFile(BufferManager& bm, VirtualFileSystem* vfs, const std::string& databasePath)
    : bm{bm}, shadowFilePath{StorageUtils::getShadowFilePath(databasePath)}, vfs{vfs},
      shadowingFH{nullptr} {
    KU_ASSERT(vfs);
}

void ShadowFile::clearShadowPage(file_idx_t originalFile, page_idx_t originalPage) {
    if (hasShadowPage(originalFile, originalPage)) {
        shadowPagesMap.at(originalFile).erase(originalPage);
        if (shadowPagesMap.at(originalFile).empty()) {
            shadowPagesMap.erase(originalFile);
        }
    }
}

page_idx_t ShadowFile::getOrCreateShadowPage(file_idx_t originalFile, page_idx_t originalPage) {
    if (hasShadowPage(originalFile, originalPage)) {
        return shadowPagesMap[originalFile][originalPage];
    }
    const auto shadowPageIdx = getOrCreateShadowingFH()->addNewPage();
    shadowPagesMap[originalFile][originalPage] = shadowPageIdx;
    shadowPageRecords.push_back({originalFile, originalPage});
    return shadowPageIdx;
}

page_idx_t ShadowFile::getShadowPage(file_idx_t originalFile, page_idx_t originalPage) const {
    KU_ASSERT(hasShadowPage(originalFile, originalPage));
    return shadowPagesMap.at(originalFile).at(originalPage);
}

void ShadowFile::applyShadowPages(ClientContext& context) const {
    const auto pageBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    page_idx_t shadowPageIdx = 1; // Skip header page.
    auto dataFileInfo = context.getStorageManager()->getDataFH()->getFileInfo();
    KU_ASSERT(shadowingFH);
    for (const auto& record : shadowPageRecords) {
        shadowingFH->readPageFromDisk(pageBuffer.get(), shadowPageIdx++);
        dataFileInfo->writeFile(pageBuffer.get(), KUZU_PAGE_SIZE,
            record.originalPageIdx * KUZU_PAGE_SIZE);
        // NOTE: We're not taking lock here, as we assume this is only called with a single thread.
        context.getMemoryManager()->getBufferManager()->updateFrameIfPageIsInFrameWithoutLock(
            record.originalFileIdx, pageBuffer.get(), record.originalPageIdx);
    }
}

void ShadowFile::replayShadowPageRecords(ClientContext& context,
    std::unique_ptr<FileInfo> fileInfo) {
    ShadowFileHeader header;
    const auto headerBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    fileInfo->readFromFile(headerBuffer.get(), KUZU_PAGE_SIZE, 0);
    memcpy(&header, headerBuffer.get(), sizeof(ShadowFileHeader));
    std::vector<ShadowPageRecord> shadowPageRecords;
    shadowPageRecords.reserve(header.numShadowPages);
    auto fileInfoPtr = fileInfo.get();
    auto reader = std::make_unique<BufferedFileReader>(std::move(fileInfo));
    reader->resetReadOffset((header.numShadowPages + 1) * KUZU_PAGE_SIZE);
    Deserializer deSer(std::move(reader));
    deSer.deserializeVector(shadowPageRecords);
    auto dataFileInfo = context.getStorageManager()->getDataFH()->getFileInfo();
    const auto pageBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    page_idx_t shadowPageIdx = 1;
    for (const auto& record : shadowPageRecords) {
        fileInfoPtr->readFromFile(pageBuffer.get(), KUZU_PAGE_SIZE, shadowPageIdx * KUZU_PAGE_SIZE);
        dataFileInfo->writeFile(pageBuffer.get(), KUZU_PAGE_SIZE,
            record.originalPageIdx * KUZU_PAGE_SIZE);
        shadowPageIdx++;
    }
}

void ShadowFile::flushAll() const {
    // Write header page to file.
    ShadowFileHeader header;
    header.numShadowPages = shadowPageRecords.size();
    const auto headerBuffer = std::make_unique<uint8_t[]>(KUZU_PAGE_SIZE);
    memcpy(headerBuffer.get(), &header, sizeof(ShadowFileHeader));
    KU_ASSERT(shadowingFH && !shadowingFH->isInMemoryMode());
    shadowingFH->writePageToFile(headerBuffer.get(), 0);
    // Flush shadow pages to file.
    shadowingFH->flushAllDirtyPagesInFrames();
    // Append shadow page records to the end of the file.
    const auto writer = std::make_shared<BufferedFileWriter>(*shadowingFH->getFileInfo());
    writer->setFileOffset(shadowingFH->getNumPages() * KUZU_PAGE_SIZE);
    Serializer ser(writer);
    KU_ASSERT(shadowPageRecords.size() + 1 == shadowingFH->getNumPages());
    ser.serializeVector(shadowPageRecords);
    writer->flush();
    // Sync the file to disk.
    writer->sync();
}

void ShadowFile::clear(BufferManager& bm) {
    KU_ASSERT(shadowingFH);
    // TODO(Guodong): We should remove shadow file here. This requires changes:
    // 1. We need to make shadow file not going through BM.
    // 2. We need to remove fileHandles held in BM, so that BM only keeps FH for the data file.
    bm.removeFilePagesFromFrames(*shadowingFH);
    shadowingFH->resetToZeroPagesAndPageCapacity();
    shadowPagesMap.clear();
    shadowPageRecords.clear();
    // Reserve header page.
    shadowingFH->addNewPage();
}

void ShadowFile::reset() {
    shadowingFH->getFileInfo()->reset();
    shadowingFH = nullptr;
    vfs->removeFileIfExists(shadowFilePath);
}

FileHandle* ShadowFile::getOrCreateShadowingFH() {
    if (!shadowingFH) {
        shadowingFH = bm.getFileHandle(shadowFilePath,
            FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS, vfs, nullptr);
        if (shadowingFH->getNumPages() == 0) {
            // Reserve the first page for the header.
            shadowingFH->addNewPage();
        }
    }
    return shadowingFH;
}

} // namespace storage
} // namespace kuzu
