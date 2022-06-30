#include "src/storage/storage_structure/include/lists/list_headers.h"

#include "spdlog/spdlog.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

ListHeaders::ListHeaders(const string& fName, uint64_t numElements) {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    headersFileHandle = make_unique<FileHandle>(
        fName + ".headers", FileHandle::O_DefaultPagedExistingDBFileCreateIfNotExists);
    // DiskArray assumes that its header page already exists. To ensure that we need to add a page
    // to the fileHandle. Currently the header page is at page 0, so we add one page here.
    headersFileHandle->addNewPage();
    headers = make_unique<InMemDiskArray<list_header_t>>(
        *headersFileHandle, LIST_HEADERS_HEADER_PAGE_IDX, numElements);
}

ListHeaders::ListHeaders(const string& listBaseFName) {
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    headersFileHandle = make_unique<FileHandle>(
        listBaseFName + ".headers", FileHandle::O_DefaultPagedExistingDBFileCreateIfNotExists);
    headers = make_unique<InMemDiskArray<list_header_t>>(
        *headersFileHandle, LIST_HEADERS_HEADER_PAGE_IDX);
    logger->trace("ListHeaders: #numNodeOffsets {}", headers->header.numElements);
};

void ListHeaders::saveToDisk() {
    headers->saveToDisk();
}

} // namespace storage
} // namespace graphflow
