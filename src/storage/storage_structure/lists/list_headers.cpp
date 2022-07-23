#include "src/storage/storage_structure/include/lists/list_headers.h"

#include "spdlog/spdlog.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

BaseListHeaders::BaseListHeaders() {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
}

ListHeadersBuilder::ListHeadersBuilder(const string& fName, uint64_t numElements)
    : BaseListHeaders() {
    fileHandle = make_unique<FileHandle>(
        fName + ".headers", FileHandle::O_DefaultPagedExistingDBFileCreateIfNotExists);
    // DiskArray assumes that its header page already exists. To ensure that we need to add a page
    // to the fileHandle. Currently, the header page is at page 0, so we add one page here.
    fileHandle->addNewPage();
    headersBuilder = make_unique<InMemDiskArrayBuilder<list_header_t>>(
        *fileHandle, LIST_HEADERS_HEADER_PAGE_IDX, numElements);
}

void ListHeadersBuilder::saveToDisk() {
    headersBuilder->saveToDisk();
}

ListHeaders::ListHeaders(const StorageStructureIDAndFName storageStructureIDAndFNameForBaseList,
    BufferManager* bufferManager, WAL* wal)
    : BaseListHeaders(), storageStructureIDAndFName(storageStructureIDAndFNameForBaseList) {
    storageStructureIDAndFName.storageStructureID.listFileID.listFileType = ListFileType::HEADERS;
    storageStructureIDAndFName.fName =
        StorageUtils::getListHeadersFName(storageStructureIDAndFNameForBaseList.fName);
    versionedFileHandle = make_unique<VersionedFileHandle>(
        storageStructureIDAndFName, FileHandle::O_DefaultPagedExistingDBFileCreateIfNotExists);
    storageStructureIDAndFName.storageStructureID.listFileID.listFileType = ListFileType::HEADERS;
    storageStructureIDAndFName.fName = versionedFileHandle->getFileInfo()->path;
    headersDiskArray = make_unique<InMemDiskArray<list_header_t>>(
        *versionedFileHandle, LIST_HEADERS_HEADER_PAGE_IDX, bufferManager, wal);
    logger->info("ListHeaders: #numNodeOffsets {}", headersDiskArray->header.numElements);
};

} // namespace storage
} // namespace graphflow
