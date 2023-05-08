#include "storage/storage_structure/lists/list_headers.h"

#include "common/utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

ListHeadersBuilder::ListHeadersBuilder(const std::string& baseListFName, uint64_t numElements) {
    fileHandle = make_unique<FileHandle>(StorageUtils::getListHeadersFName(baseListFName),
        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    // DiskArray assumes that its header page already exists. To ensure that we need to add a page
    // to the fileHandle. Currently, the header page is at page 0, so we add one page here.
    fileHandle->addNewPage();
    headersBuilder = std::make_unique<InMemDiskArrayBuilder<csr_offset_t>>(
        *fileHandle, LIST_HEADERS_HEADER_PAGE_IDX, numElements, true /* setToZero */);
}

ListHeaders::ListHeaders(const StorageStructureIDAndFName& storageStructureIDAndFNameForBaseList,
    BufferManager* bufferManager, WAL* wal)
    : storageStructureIDAndFName(storageStructureIDAndFNameForBaseList) {
    storageStructureIDAndFName.storageStructureID.listFileID.listFileType = ListFileType::HEADERS;
    storageStructureIDAndFName.fName =
        StorageUtils::getListHeadersFName(storageStructureIDAndFNameForBaseList.fName);
    fileHandle = bufferManager->getBMFileHandle(storageStructureIDAndFName.fName,
        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE);
    storageStructureIDAndFName.storageStructureID.listFileID.listFileType = ListFileType::HEADERS;
    storageStructureIDAndFName.fName = fileHandle->getFileInfo()->path;
    headersDiskArray = std::make_unique<InMemDiskArray<csr_offset_t>>(*fileHandle,
        storageStructureIDAndFName.storageStructureID, LIST_HEADERS_HEADER_PAGE_IDX, bufferManager,
        wal);
}

} // namespace storage
} // namespace kuzu
