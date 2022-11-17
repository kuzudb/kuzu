#pragma once

#include <fcntl.h>

#include "src/common/include/vector/value_vector.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/versioned_file_handle.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/storage_structure/include/storage_structure.h"
#include "src/storage/storage_structure/include/storage_structure_utils.h"
#include "src/storage/wal/include/wal.h"
#include "src/transaction/include/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

class DiskOverflowFile : public StorageStructure {
    friend class StringPropertyColumn;

public:
    DiskOverflowFile(const StorageStructureIDAndFName& storageStructureIDAndFNameOfMainDBFile,
        BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : StorageStructure(
              constructOverflowStorageStructureIDAndFName(storageStructureIDAndFNameOfMainDBFile),
              bufferManager, isInMemory, wal),
          loggedNewOverflowFileNextBytePosRecord{false} {
        nextBytePosToWriteTo = fileHandle.getNumPages() * DEFAULT_PAGE_SIZE;
    }

    ~DiskOverflowFile() {
        if (isInMemory_) {
            StorageStructureUtils::unpinEachPageOfFile(fileHandle, bufferManager);
        }
    }

    static inline StorageStructureIDAndFName constructOverflowStorageStructureIDAndFName(
        const StorageStructureIDAndFName& storageStructureIDAndFNameForMainDBFile) {
        StorageStructureIDAndFName copy = storageStructureIDAndFNameForMainDBFile;
        copy.storageStructureID.isOverflow = true;
        copy.fName =
            StorageUtils::getOverflowFileName(storageStructureIDAndFNameForMainDBFile.fName);
        return copy;
    }

    void readStringsToVector(TransactionType trxType, ValueVector& valueVector);
    void readStringToVector(
        TransactionType trxType, ku_string_t& kuStr, InMemOverflowBuffer& inMemOverflowBuffer);

    inline void scanSingleStringOverflow(
        TransactionType trxType, ValueVector& vector, uint64_t vectorPos) {
        assert(vector.dataType.typeID == STRING && !vector.isNull(vectorPos));
        auto& kuString = ((ku_string_t*)vector.getData())[vectorPos];
        readStringToVector(trxType, kuString, vector.getOverflowBuffer());
    }
    void scanSequentialStringOverflow(TransactionType trxType, ValueVector& vector);
    inline void scanSingleListOverflow(
        TransactionType trxType, ValueVector& vector, uint64_t vectorPos) {
        assert(vector.dataType.typeID == LIST && !vector.isNull(vectorPos));
        auto& kuList = ((ku_list_t*)vector.getData())[vectorPos];
        readListToVector(trxType, kuList, vector.dataType, vector.getOverflowBuffer());
    }

    void readListsToVector(TransactionType trxType, ValueVector& valueVector);

    string readString(TransactionType trxType, const ku_string_t& str);
    vector<Literal> readList(
        TransactionType trxType, const ku_list_t& listVal, const DataType& dataType);

    ku_string_t writeString(const char* rawString);
    void writeStringOverflowAndUpdateOverflowPtr(
        const ku_string_t& strToWriteFrom, ku_string_t& strToWriteTo);
    void writeListOverflowAndUpdateOverflowPtr(const ku_list_t& listToWriteFrom,
        ku_list_t& listToWriteTo, const DataType& elementDataType);

    inline void resetNextBytePosToWriteTo(uint64_t nextBytePosToWriteTo_) {
        nextBytePosToWriteTo = nextBytePosToWriteTo_;
    }

    void resetLoggedNewOverflowFileNextBytePosRecord() {
        loggedNewOverflowFileNextBytePosRecord = false;
    }

private:
    void addNewPageIfNecessaryWithoutLock(uint32_t numBytesToAppend);
    void readListToVector(TransactionType trxType, ku_list_t& kuList, const DataType& dataType,
        InMemOverflowBuffer& inMemOverflowBuffer);
    void setStringOverflowWithoutLock(
        const char* inMemSrcStr, uint64_t len, ku_string_t& diskDstString);
    void setListRecursiveIfNestedWithoutLock(
        const ku_list_t& inMemSrcList, ku_list_t& diskDstList, const DataType& dataType);
    void logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();

private:
    // This is the index of the last free byte to which we can write.
    uint64_t nextBytePosToWriteTo;
    // Mtx is obtained if multiple threads want to write to the overflows to coordinate them.
    // We cannot directly coordinate using the locks of the pages in the overflow fileHandle.
    // This is because multiple threads will be trying to edit the nextBytePosToWriteTo.
    // For simplicity, currently only a single thread can update overflow pages at ant time. See the
    // note in writeStringOverflowAndUpdateOverflowPtr for more details.
    mutex mtx;
    bool loggedNewOverflowFileNextBytePosRecord;
};

} // namespace storage
} // namespace kuzu
