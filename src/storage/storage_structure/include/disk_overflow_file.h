#pragma once

#include "fcntl.h"

#include "src/common/include/vector/value_vector.h"
#include "src/storage/buffer_manager/include/buffer_manager.h"
#include "src/storage/buffer_manager/include/versioned_file_handle.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/storage_structure/include/storage_structure.h"
#include "src/storage/storage_structure/include/storage_structure_utils.h"
#include "src/storage/wal/include/wal.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::common;
using namespace graphflow::transaction;

namespace graphflow {
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

    // TODO(Semih/Guodong): Remove funcs without trx once all read string calls use a trx.
    inline void readStringsToVector(ValueVector& valueVector) {
        Transaction tmpTransaction(READ_ONLY, -1);
        readStringsToVector(&tmpTransaction, valueVector);
    }
    inline void readStringToVector(gf_string_t& gfStr, InMemOverflowBuffer& inMemOverflowBuffer) {
        Transaction tmpTransaction(READ_ONLY, -1);
        readStringToVector(&tmpTransaction, gfStr, inMemOverflowBuffer);
    }

    void readStringsToVector(Transaction* transaction, ValueVector& valueVector);
    void readStringToVector(
        Transaction* transaction, gf_string_t& gfStr, InMemOverflowBuffer& inMemOverflowBuffer);

    inline void scanSingleStringOverflow(
        Transaction* transaction, ValueVector& vector, uint64_t vectorPos) {
        assert(vector.dataType.typeID == STRING && !vector.isNull(vectorPos));
        auto& gfString = ((gf_string_t*)vector.values)[vectorPos];
        readStringToVector(transaction, gfString, vector.getOverflowBuffer());
    }
    void scanSequentialStringOverflow(Transaction* transaction, ValueVector& vector);

    void readListsToVector(ValueVector& valueVector);

    // TODO(Semih/Guodong): Remove funcs without trx once all read string calls use a trx.
    string readString(const gf_string_t& str) {
        Transaction tmpTransaction(READ_ONLY, -1);
        return readString(&tmpTransaction, str);
    }

    string readString(Transaction* transaction, const gf_string_t& str);
    vector<Literal> readList(const gf_list_t& listVal, const DataType& dataType);
    void writeStringOverflowAndUpdateOverflowPtr(
        const gf_string_t& strToWriteFrom, gf_string_t& strToWriteTo);
    void writeListOverflowAndUpdateOverflowPtr(const gf_list_t& listToWriteFrom,
        gf_list_t& listToWriteTo, const DataType& elementDataType);

    inline void resetNextBytePosToWriteTo(uint64_t nextBytePosToWriteTo_) {
        nextBytePosToWriteTo = nextBytePosToWriteTo_;
    }

    void resetLoggedNewOverflowFileNextBytePosRecord() {
        loggedNewOverflowFileNextBytePosRecord = false;
    }

private:
    void addNewPageIfNecessaryWithoutLock(uint32_t numBytesToAppend);
    void readListToVector(
        gf_list_t& gfList, const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer);
    void setStringOverflowWithoutLock(const gf_string_t& src, gf_string_t& dst);
    void setListRecursiveIfNestedWithoutLock(
        const gf_list_t& src, gf_list_t& dst, const DataType& dataType);
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
} // namespace graphflow