#include "src/storage/storage_structure/include/lists/list_headers.h"

#include "spdlog/spdlog.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

BaseListHeaders::BaseListHeaders(const string& listBaseFName) {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    fileHandle = make_unique<FileHandle>(
        listBaseFName + ".headers", FileHandle::O_DefaultPagedExistingDBFileCreateIfNotExists);
}
ListHeadersBuilder::ListHeadersBuilder(const string& fName, uint64_t numElements)
    : BaseListHeaders(fName) {
    fileHandle = make_unique<FileHandle>(
        fName + ".headers", FileHandle::O_DefaultPagedExistingDBFileCreateIfNotExists);
    // DiskArray assumes that its header page already exists. To ensure that we need to add a page
    // to the fileHandle. Currently the header page is at page 0, so we add one page here.
    fileHandle->addNewPage();
    headersBuilder = make_unique<InMemDiskArrayBuilder<list_header_t>>(
        *fileHandle, LIST_HEADERS_HEADER_PAGE_IDX, numElements);
}

void ListHeadersBuilder::saveToDisk() {
    headersBuilder->saveToDisk();
}

ListHeaders::ListHeaders(const StorageStructureIDAndFName storageStructureIDAndFNameForBaseList)
    : BaseListHeaders(storageStructureIDAndFNameForBaseList.fName),
      storageStructureIDAndFName(storageStructureIDAndFNameForBaseList) {
    storageStructureIDAndFName.storageStructureID.listFileID.listFileType = ListFileType::HEADERS;
    storageStructureIDAndFName.fName = fileHandle->getFileInfo()->path;
    headers = make_unique<InMemDiskArray<list_header_t>>(*fileHandle, LIST_HEADERS_HEADER_PAGE_IDX);
    logger->trace("ListHeaders: #numNodeOffsets {}", headers->header.numElements);
};

} // namespace storage
} // namespace graphflow
