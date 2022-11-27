#pragma once

#include <functional>

#include "common/configs.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/versioned_file_handle.h"
#include "storage/node_id_compression_scheme.h"
#include "storage/storage_structure/storage_structure_utils.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

class ListsUpdateIterator;

class StorageStructure {
    friend class ListsUpdateIterator;

public:
    StorageStructure(const StorageStructureIDAndFName& storageStructureIDAndFName,
        BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : logger{LoggerUtils::getOrCreateLogger("storage")},
          fileHandle{storageStructureIDAndFName, FileHandle::O_PERSISTENT_FILE_NO_CREATE},
          bufferManager{bufferManager}, isInMemory_{isInMemory}, wal{wal} {
        if (isInMemory) {
            StorageStructureUtils::pinEachPageOfFile(fileHandle, bufferManager);
        }
    }

    inline VersionedFileHandle* getFileHandle() { return &fileHandle; }

protected:
    void addNewPageToFileHandle();

    // If necessary creates a second version (backed by the WAL) of a page that contains the value
    // that will be written to. The position of the value, which determines the original page to
    // update, is computed from the given elementOffset and numElementsPerPage argument. Obtains
    // *and does not release* the lock original page. Pins and updates the WAL version of the
    // page. Note that caller must ensure to unpin and release the WAL version of the page by
    // calling StorageStructure::unpinWALPageAndReleaseOriginalPageLock.
    WALPageIdxPosInPageAndFrame createWALVersionOfPageIfNecessaryForElement(
        uint64_t elementOffset, uint64_t numElementsPerPage);

protected:
    shared_ptr<spdlog::logger> logger;
    VersionedFileHandle fileHandle;
    BufferManager& bufferManager;
    bool isInMemory_;
    WAL* wal;
};

// BaseColumnOrList is the parent class of Column and Lists. It abstracts the state and
// functions that are common in both column and lists, like, 1) layout info (size of a unit of
// element and number of elements that can be accommodated in a page), 2) getting pageIdx and
// pageOffset of an element and, 3) reading from pages.
class BaseColumnOrList : public StorageStructure {

public:
    DataTypeID getDataTypeId() const { return dataType.typeID; }

    // Maps the position of element in page to its byte offset in page.
    // TODO(Everyone): we should slowly get rid of this function.
    inline uint16_t mapElementPosToByteOffset(uint16_t pageElementPos) const {
        return pageElementPos * elementSize;
    }

protected:
    BaseColumnOrList(const StorageStructureIDAndFName& storageStructureIDAndFName,
        DataType dataType, const size_t& elementSize, BufferManager& bufferManager,
        bool hasNULLBytes, bool isInMemory, WAL* wal);

    virtual ~BaseColumnOrList() {
        if (isInMemory_) {
            StorageStructureUtils::unpinEachPageOfFile(fileHandle, bufferManager);
        }
    }

    void readBySequentialCopy(Transaction* transaction, const shared_ptr<ValueVector>& vector,
        PageElementCursor& cursor,
        const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper);

    void readBySequentialCopyWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& vector, PageElementCursor& cursor,
        const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper);

    void readNodeIDsBySequentialCopy(Transaction* transaction,
        const shared_ptr<ValueVector>& valueVector, PageElementCursor& cursor,
        const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper,
        NodeIDCompressionScheme nodeIDCompressionScheme, bool isAdjLists);

    void readNodeIDsBySequentialCopyWithSelState(Transaction* transaction,
        const shared_ptr<ValueVector>& valueVector, PageElementCursor& cursor,
        const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper,
        NodeIDCompressionScheme nodeIDCompressionScheme);

    void readNodeIDsFromAPageBySequentialCopy(Transaction* transaction,
        const shared_ptr<ValueVector>& vector, uint64_t vectorStartPos, page_idx_t physicalPageIdx,
        uint16_t pagePosOfFirstElement, uint64_t numValuesToRead,
        NodeIDCompressionScheme& nodeIDCompressionScheme, bool isAdjLists);

    void readSingleNullBit(const shared_ptr<ValueVector>& valueVector, const uint8_t* frame,
        uint64_t elementPos, uint64_t offsetInVector) const;

    void setNullBitOfAPosInFrame(uint8_t* frame, uint16_t elementPos, bool isNull) const;

private:
    void readAPageBySequentialCopy(Transaction* transaction, const shared_ptr<ValueVector>& vector,
        uint64_t vectorStartPos, page_idx_t physicalPageIdx, uint16_t pagePosOfFirstElement,
        uint64_t numValuesToRead);

    void readNullBitsFromAPage(const shared_ptr<ValueVector>& valueVector, const uint8_t* frame,
        uint64_t posInPage, uint64_t posInVector, uint64_t numBitsToRead) const;

public:
    DataType dataType;
    size_t elementSize;
    uint32_t numElementsPerPage;
};

} // namespace storage
} // namespace kuzu
