#pragma once

#include <fcntl.h>

#include "common/vector/value_vector.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/storage_structure/storage_structure.h"
#include "storage/storage_structure/storage_structure_utils.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class DiskOverflowFile : public StorageStructure {
    friend class StringPropertyColumn;

public:
    DiskOverflowFile(const StorageStructureIDAndFName& storageStructureIDAndFNameOfMainDBFile,
        BufferManager& bufferManager, WAL* wal)
        : StorageStructure(
              constructOverflowStorageStructureIDAndFName(storageStructureIDAndFNameOfMainDBFile),
              bufferManager, wal),
          loggedNewOverflowFileNextBytePosRecord{false} {
        nextBytePosToWriteTo =
            fileHandle->getNumPages() * common::BufferPoolConstants::PAGE_4KB_SIZE;
    }

    static inline StorageStructureIDAndFName constructOverflowStorageStructureIDAndFName(
        const StorageStructureIDAndFName& storageStructureIDAndFNameForMainDBFile) {
        StorageStructureIDAndFName copy = storageStructureIDAndFNameForMainDBFile;
        copy.storageStructureID.isOverflow = true;
        copy.fName =
            StorageUtils::getOverflowFileName(storageStructureIDAndFNameForMainDBFile.fName);
        return copy;
    }

    void scanStrings(transaction::TransactionType trxType, common::ValueVector& valueVector);

    inline void scanSingleStringOverflow(
        transaction::TransactionType trxType, common::ValueVector& vector, uint64_t vectorPos) {
        assert(vector.dataType.typeID == common::STRING && !vector.isNull(vectorPos));
        auto& kuString = ((common::ku_string_t*)vector.getData())[vectorPos];
        lookupString(trxType, kuString, vector.getOverflowBuffer());
    }
    inline void scanSingleListOverflow(
        transaction::TransactionType trxType, common::ValueVector& vector, uint64_t vectorPos) {
        assert(vector.dataType.typeID == common::VAR_LIST && !vector.isNull(vectorPos));
        auto& kuList = ((common::ku_list_t*)vector.getData())[vectorPos];
        readListToVector(trxType, kuList, vector.dataType, vector.getOverflowBuffer());
    }

    void readListsToVector(transaction::TransactionType trxType, common::ValueVector& valueVector);

    std::string readString(transaction::TransactionType trxType, const common::ku_string_t& str);
    std::vector<std::unique_ptr<common::Value>> readList(transaction::TransactionType trxType,
        const common::ku_list_t& listVal, const common::DataType& dataType);

    common::ku_string_t writeString(const char* rawString);
    void writeStringOverflowAndUpdateOverflowPtr(
        const common::ku_string_t& strToWriteFrom, common::ku_string_t& strToWriteTo);
    void writeListOverflowAndUpdateOverflowPtr(const common::ku_list_t& listToWriteFrom,
        common::ku_list_t& listToWriteTo, const common::DataType& elementDataType);

    inline void resetNextBytePosToWriteTo(uint64_t nextBytePosToWriteTo_) {
        nextBytePosToWriteTo = nextBytePosToWriteTo_;
    }

    void resetLoggedNewOverflowFileNextBytePosRecord() {
        loggedNewOverflowFileNextBytePosRecord = false;
    }

private:
    struct OverflowPageCache {
        BMFileHandle* bmFileHandle = nullptr;
        common::page_idx_t pageIdx = UINT32_MAX;
        uint8_t* frame = nullptr;
    };
    void lookupString(transaction::TransactionType trxType, common::ku_string_t& kuStr,
        common::InMemOverflowBuffer& inMemOverflowBuffer);
    void lookupString(transaction::TransactionType trxType, common::ku_string_t& kuStr,
        common::InMemOverflowBuffer& inMemOverflowBuffer, OverflowPageCache& overflowPageCache);
    void addNewPageIfNecessaryWithoutLock(uint32_t numBytesToAppend);
    void readListToVector(transaction::TransactionType trxType, common::ku_list_t& kuList,
        const common::DataType& dataType, common::InMemOverflowBuffer& inMemOverflowBuffer);
    void setStringOverflowWithoutLock(
        const char* inMemSrcStr, uint64_t len, common::ku_string_t& diskDstString);
    void setListRecursiveIfNestedWithoutLock(const common::ku_list_t& inMemSrcList,
        common::ku_list_t& diskDstList, const common::DataType& dataType);
    void logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();
    void readValuesInList(transaction::TransactionType trxType, const common::DataType& dataType,
        std::vector<std::unique_ptr<common::Value>>& retValues, uint32_t numBytesOfSingleValue,
        uint64_t numValuesInList, PageByteCursor& cursor, uint8_t* frame);
    void pinOverflowPageCache(BMFileHandle* bmFileHandleToPin, common::page_idx_t pageIdxToPin,
        OverflowPageCache& overflowPageCache);
    void unpinOverflowPageCache(OverflowPageCache& overflowPageCache);

private:
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
