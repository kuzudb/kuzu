#pragma once

#include <fcntl.h>

#include "common/types/ku_list.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/storage_structure/db_file_utils.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class DiskOverflowFile {

public:
    DiskOverflowFile(const DBFileIDAndName& dbFileIdAndName, BufferManager* bufferManager, WAL* wal)
        : bufferManager{bufferManager}, wal{wal}, loggedNewOverflowFileNextBytePosRecord{false} {
        auto overflowFileIDAndName = constructDBFileIDAndName(dbFileIdAndName);
        dbFileID = overflowFileIDAndName.dbFileID;
        fileHandle = bufferManager->getBMFileHandle(overflowFileIDAndName.fName,
            FileHandle::O_PERSISTENT_FILE_NO_CREATE,
            BMFileHandle::FileVersionedType::VERSIONED_FILE);
        nextBytePosToWriteTo =
            fileHandle->getNumPages() * common::BufferPoolConstants::PAGE_4KB_SIZE;
    }

    void readListToVector(transaction::TransactionType trxType, common::ku_list_t& kuList,
        common::ValueVector* vector, uint64_t pos);
    std::string readString(transaction::TransactionType trxType, const common::ku_string_t& str);
    std::vector<std::unique_ptr<common::Value>> readList(transaction::TransactionType trxType,
        const common::ku_list_t& listVal, const common::LogicalType& dataType);

    common::ku_string_t writeString(const char* rawString);
    void writeStringOverflowAndUpdateOverflowPtr(
        const common::ku_string_t& strToWriteFrom, common::ku_string_t& strToWriteTo);
    void writeListOverflowAndUpdateOverflowPtr(const common::ku_list_t& listToWriteFrom,
        common::ku_list_t& listToWriteTo, const common::LogicalType& valueType);

    inline BMFileHandle* getFileHandle() { return fileHandle.get(); }
    inline void resetNextBytePosToWriteTo(uint64_t nextBytePosToWriteTo_) {
        nextBytePosToWriteTo = nextBytePosToWriteTo_;
    }
    void resetLoggedNewOverflowFileNextBytePosRecord() {
        loggedNewOverflowFileNextBytePosRecord = false;
    }

private:
    static inline DBFileIDAndName constructDBFileIDAndName(
        const DBFileIDAndName& dbFileIdAndNameForMainDBFile) {
        DBFileIDAndName copy = dbFileIdAndNameForMainDBFile;
        copy.dbFileID.isOverflow = true;
        copy.fName = StorageUtils::getOverflowFileName(dbFileIdAndNameForMainDBFile.fName);
        return copy;
    }

private:
    struct OverflowPageCache {
        BMFileHandle* bmFileHandle = nullptr;
        common::page_idx_t pageIdx = UINT32_MAX;
        uint8_t* frame = nullptr;
    };
    void lookupString(transaction::TransactionType trxType, common::ValueVector* vector,
        common::ku_string_t& dstStr, OverflowPageCache& overflowPageCache);
    void addNewPageIfNecessaryWithoutLock(uint32_t numBytesToAppend);
    void setStringOverflowWithoutLock(
        const char* inMemSrcStr, uint64_t len, common::ku_string_t& diskDstString);
    void setListRecursiveIfNestedWithoutLock(const common::ku_list_t& inMemSrcList,
        common::ku_list_t& diskDstList, const common::LogicalType& dataType);
    void logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();
    void readValuesInList(transaction::TransactionType trxType, const common::LogicalType& dataType,
        std::vector<std::unique_ptr<common::Value>>& retValues, uint32_t numBytesOfSingleValue,
        uint64_t numValuesInList, PageByteCursor& cursor, uint8_t* frame);
    void pinOverflowPageCache(BMFileHandle* bmFileHandleToPin, common::page_idx_t pageIdxToPin,
        OverflowPageCache& overflowPageCache);
    void unpinOverflowPageCache(OverflowPageCache& overflowPageCache);

    // If necessary creates a second version (backed by the WAL) of a page that contains the value
    // that will be written to. The position of the value, which determines the original page to
    // update, is computed from the given elementOffset and numElementsPerPage argument. Obtains
    // *and does not release* the lock original page. Pins and updates the WAL version of the
    // page. Note that caller must ensure to unpin and release the WAL version of the page by
    // calling StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock.
    WALPageIdxPosInPageAndFrame createWALVersionOfPageIfNecessaryForElement(
        uint64_t elementOffset, uint64_t numElementsPerPage);

private:
    DBFileID dbFileID;
    std::unique_ptr<BMFileHandle> fileHandle;
    BufferManager* bufferManager;
    WAL* wal;
    // This is the index of the last free byte to which we can write.
    uint64_t nextBytePosToWriteTo;
    // Mtx is obtained if multiple threads want to write to the overflows to coordinate them.
    // We cannot directly coordinate using the locks of the pages in the overflow fileHandle.
    // This is because multiple threads will be trying to edit the nextBytePosToWriteTo.
    // For simplicity, currently only a single thread can update overflow pages at ant time. See the
    // note in writeStringOverflowAndUpdateOverflowPtr for more details.
    std::mutex mtx;
    bool loggedNewOverflowFileNextBytePosRecord;
};

} // namespace storage
} // namespace kuzu
