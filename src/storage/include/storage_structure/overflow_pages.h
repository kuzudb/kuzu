#pragma once

#include "fcntl.h"

#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/storage_structure/storage_structure.h"
#include "src/storage/include/storage_structure_utils.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/include/versioned_file_handle.h"
#include "src/storage/include/wal/wal.h"
#include "src/transaction/include/transaction.h"

using namespace graphflow::common;
using namespace graphflow::transaction;

namespace graphflow {
namespace storage {

class OverflowPages : public StorageStructure {

public:
    explicit OverflowPages(const StorageStructureIDAndFName& storageStructureIDAndFNameOfMainDBFile,
        BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : StorageStructure(
              constructOverflowStorageStructureIDAndFName(storageStructureIDAndFNameOfMainDBFile),
              bufferManager, isInMemory, wal) {
        nextBytePosToWriteTo = fileHandle.getNumPages() * DEFAULT_PAGE_SIZE;
    }

    // TODO(Semih/Guodong): This overloaded constructor exists for storage structures that hold
    // overflow pages but is not yet updatable, such as hash index or rel property lists storing
    // strings. Currently, it creates a dummy StorageStructureID. This should be removed when we
    // support updates for those structures too.
    explicit OverflowPages(const string& fName, BufferManager& bufferManager, bool isInMemory)
        : OverflowPages(
              StorageStructureIDAndFName(
                  StorageStructureID::newStructuredNodePropertyMainColumnID(-1, -1), fName),
              bufferManager, isInMemory, nullptr /* wal is null */) {}

    ~OverflowPages() {
        if (isInMemory_) {
            StorageStructureUtils::unpinEachPageOfFile(fileHandle, bufferManager);
        }
    }

    static inline StorageStructureIDAndFName constructOverflowStorageStructureIDAndFName(
        const StorageStructureIDAndFName& storageStructureIDAndFNameForMainDBFile) {
        StorageStructureID newOverflowStorageStructureID =
            storageStructureIDAndFNameForMainDBFile.storageStructureID;
        newOverflowStorageStructureID.isOverflow = true;
        auto newFilePath =
            StorageUtils::getOverflowPagesFName(storageStructureIDAndFNameForMainDBFile.fName);
        return StorageStructureIDAndFName(newOverflowStorageStructureID, newFilePath);
    }

    // TODO(Semih/Guodong): Remove funcs without trx once all read string calls use a trx.
    void readStringsToVector(ValueVector& valueVector);
    void readStringToVector(gf_string_t& gfStr, OverflowBuffer& overflowBuffer);
    void readStringsToVector(Transaction* transaction, ValueVector& valueVector);
    void readStringToVector(
        Transaction* transaction, gf_string_t& gfStr, OverflowBuffer& overflowBuffer);
    void readListsToVector(ValueVector& valueVector);
    string readString(const gf_string_t& str);
    vector<Literal> readList(const gf_list_t& listVal, const DataType& dataType);
    void writeStringOverflowAndUpdateOverflowPtr(
        const gf_string_t& strToWriteFrom, gf_string_t& strToWriteTo);
    void writeListOverflowAndUpdateOverflowPtr(const gf_list_t& listToWriteFrom,
        gf_list_t& listToWriteTo, const DataType& elementDataType);

private:
    void addNewPageIfNecessaryWithoutLock(uint32_t numBytesToAppend);
    void readListToVector(
        gf_list_t& gfList, const DataType& dataType, OverflowBuffer& overflowBuffer);
    void setStringOverflowWithoutLock(const gf_string_t& src, gf_string_t& dst);
    void setListRecursiveIfNestedWithoutLock(
        const gf_list_t& src, gf_list_t& dst, const DataType& dataType);

private:
    // This is the index of the last free byte to which we can write.
    uint64_t nextBytePosToWriteTo;
    // Mtx is obtained if multiple threads want to write to the overflows to coordinate them.
    // We cannot directly coordinate using the locks of the pages in the overflow fileHandle.
    // This is because multiple threads will be trying to edit the nextBytePosToWriteTo.
    // For simplicity, currently only a single thread can update overflow pages at ant time. See the
    // note in writeStringOverflowAndUpdateOverflowPtr for more details.
    mutex mtx;
};

} // namespace storage
} // namespace graphflow