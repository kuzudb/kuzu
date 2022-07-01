#include "src/storage/storage_structure/include/lists/list_headers.h"

#include "spdlog/spdlog.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

BaseListHeaders::BaseListHeaders(const string& fName) {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    headersFileHandle = make_unique<FileHandle>(
        fName + ".headers", FileHandle::O_DefaultPagedExistingDBFileCreateIfNotExists);
}
ListHeadersBuilder::ListHeadersBuilder(const string& fName, uint64_t numElements)
    : BaseListHeaders(fName) {
    // DiskArray assumes that its header page already exists. To ensure that we need to add a page
    // to the fileHandle. Currently the header page is at page 0, so we add one page here.
    headersFileHandle->addNewPage();
    headersBuilder = make_unique<InMemDiskArrayBuilder<list_header_t>>(
        *headersFileHandle, LIST_HEADERS_HEADER_PAGE_IDX, numElements);
}

void ListHeadersBuilder::saveToDisk() {
    headersBuilder->saveToDisk();
}

ListHeaders::ListHeaders(const string& fName) : BaseListHeaders(fName) {
    headers = make_unique<InMemDiskArray<list_header_t>>(
        *headersFileHandle, LIST_HEADERS_HEADER_PAGE_IDX);
    logger->trace("ListHeaders: #numNodeOffsets {}", headers->header.numElements);
};

} // namespace storage
} // namespace graphflow
