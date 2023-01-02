#include "storage/storage_structure/lists/list_headers.h"

#include "common/utils.h"
#include "spdlog/spdlog.h"

namespace kuzu {
namespace storage {

BaseListHeaders::BaseListHeaders() {
    logger = LoggerUtils::getOrCreateLogger("storage");
}

ListHeadersBuilder::ListHeadersBuilder(const string& baseListFName, uint64_t numElements)
    : BaseListHeaders() {
    fileHandle = make_unique<FileHandle>(StorageUtils::getListHeadersFName(baseListFName),
        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
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
        storageStructureIDAndFName, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    storageStructureIDAndFName.storageStructureID.listFileID.listFileType = ListFileType::HEADERS;
    storageStructureIDAndFName.fName = versionedFileHandle->getFileInfo()->path;
    headersDiskArray = make_unique<InMemDiskArray<list_header_t>>(
        *versionedFileHandle, LIST_HEADERS_HEADER_PAGE_IDX, bufferManager, wal);
    logger->info("ListHeaders: #numNodeOffsets {}", headersDiskArray->header.numElements);
};

} // namespace storage
} // namespace kuzu
