#pragma once

#include <functional>
#include <utility>

#include "common/constants.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/storage_structure/storage_structure_utils.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

typedef uint64_t chunk_idx_t;

class ListsUpdateIterator;

class StorageStructure {
    friend class ListsUpdateIterator;

public:
    StorageStructure() = default;

    StorageStructure(const StorageStructureIDAndFName& storageStructureIDAndFName,
        BufferManager* bufferManager, WAL* wal)
        : logger{common::LoggerUtils::getLogger(common::LoggerConstants::LoggerEnum::STORAGE)},
          storageStructureID{storageStructureIDAndFName.storageStructureID},
          bufferManager{bufferManager}, wal{wal} {
        assert(!storageStructureIDAndFName.fName.empty());
        fileHandle = bufferManager->getBMFileHandle(storageStructureIDAndFName.fName,
            FileHandle::O_PERSISTENT_FILE_NO_CREATE,
            BMFileHandle::FileVersionedType::VERSIONED_FILE);
    }

    virtual ~StorageStructure() = default;

    inline BMFileHandle* getFileHandle() { return fileHandle.get(); }

    // check if val is in range [start, end)
    static inline bool isInRange(uint64_t val, uint64_t start, uint64_t end) {
        return val >= start && val < end;
    }

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
    std::shared_ptr<spdlog::logger> logger;
    StorageStructureID storageStructureID;
    std::unique_ptr<BMFileHandle> fileHandle;
    BufferManager* bufferManager;
    WAL* wal;
};

// BaseColumnOrList is the parent class of Column and Lists. It abstracts the state and
// functions that are common in both column and lists, like, 1) layout info (size of a unit of
// element and number of elements that can be accommodated in a page), 2) getting pageIdx and
// pageOffset of an element and, 3) reading from pages.
class BaseColumnOrList : public StorageStructure {

public:
    explicit BaseColumnOrList(common::LogicalType dataType) : dataType{std::move(dataType)} {}

    // Maps the position of element in page to its byte offset in page.
    // TODO(Everyone): we should slowly get rid of this function.
    inline uint16_t mapElementPosToByteOffset(uint16_t pageElementPos) const {
        return pageElementPos * elementSize;
    }

    inline const uint8_t* getNullBufferInPage(const uint8_t* pageFrame) const {
        return pageFrame + numElementsPerPage * elementSize;
    }

protected:
    inline uint64_t getElemByteOffset(uint64_t elemPosInPage) const {
        return elemPosInPage * elementSize;
    }

    BaseColumnOrList(const StorageStructureIDAndFName& storageStructureIDAndFName,
        common::LogicalType dataType, const size_t& elementSize, BufferManager* bufferManager,
        bool hasInlineNullBytes, WAL* wal);

    void readBySequentialCopy(transaction::Transaction* transaction, common::ValueVector* vector,
        PageElementCursor& cursor,
        const std::function<common::page_idx_t(common::page_idx_t)>& logicalToPhysicalPageMapper);

    void readInternalIDsBySequentialCopy(transaction::Transaction* transaction,
        common::ValueVector* vector, PageElementCursor& cursor,
        const std::function<common::page_idx_t(common::page_idx_t)>& logicalToPhysicalPageMapper,
        common::table_id_t commonTableID, bool hasNoNullGuarantee);

    void readInternalIDsFromAPageBySequentialCopy(transaction::Transaction* transaction,
        common::ValueVector* vector, uint64_t vectorStartPos, common::page_idx_t physicalPageIdx,
        uint16_t pagePosOfFirstElement, uint64_t numValuesToRead, common::table_id_t commonTableID,
        bool hasNoNullGuarantee);

    void readNullBitsFromAPage(common::ValueVector* valueVector, const uint8_t* frame,
        uint64_t posInPage, uint64_t posInVector, uint64_t numBitsToRead) const;

private:
    void readAPageBySequentialCopy(transaction::Transaction* transaction,
        common::ValueVector* vector, uint64_t vectorStartPos, common::page_idx_t physicalPageIdx,
        uint16_t pagePosOfFirstElement, uint64_t numValuesToRead);

public:
    common::LogicalType dataType;
    size_t elementSize;
    uint32_t numElementsPerPage;
};

} // namespace storage
} // namespace kuzu
