#pragma once

#include <fcntl.h>

#include "common/constants.h"
#include "common/types/types.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/storage_utils.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class DiskOverflowFile {

public:
    DiskOverflowFile(const DBFileIDAndName& dbFileIdAndName, BufferManager* bufferManager, WAL* wal,
        bool readOnly, common::VirtualFileSystem* vfs)
        : bufferManager{bufferManager}, wal{wal}, loggedNewOverflowFileNextBytePosRecord{false} {
        auto overflowFileIDAndName = constructDBFileIDAndName(dbFileIdAndName);
        dbFileID = overflowFileIDAndName.dbFileID;
        fileHandle = bufferManager->getBMFileHandle(overflowFileIDAndName.fName,
            readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                       FileHandle::O_PERSISTENT_FILE_NO_CREATE,
            BMFileHandle::FileVersionedType::VERSIONED_FILE, vfs);
        nextPosToWriteTo.elemPosInPage = 0;
        nextPosToWriteTo.pageIdx = fileHandle->getNumPages();
    }

    std::string readString(transaction::TransactionType trxType, const common::ku_string_t& str);

    common::ku_string_t writeString(std::string_view rawString);
    inline common::ku_string_t writeString(const char* rawString) {
        return writeString(std::string_view(rawString));
    }

    inline BMFileHandle* getFileHandle() { return fileHandle.get(); }
    inline void resetNextBytePosToWriteTo(uint64_t nextBytePosToWriteTo_) {
        nextPosToWriteTo.elemPosInPage =
            nextBytePosToWriteTo_ % common::BufferPoolConstants::PAGE_4KB_SIZE;
        nextPosToWriteTo.pageIdx =
            nextBytePosToWriteTo_ / common::BufferPoolConstants::PAGE_4KB_SIZE;
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
    bool addNewPageIfNecessaryWithoutLock(uint32_t numBytesToAppend);
    void setStringOverflowWithoutLock(
        const char* inMemSrcStr, uint64_t len, common::ku_string_t& diskDstString);
    void logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();

private:
    static const common::page_idx_t END_OF_PAGE =
        common::BufferPoolConstants::PAGE_4KB_SIZE - sizeof(common::page_idx_t);
    DBFileID dbFileID;
    std::unique_ptr<BMFileHandle> fileHandle;
    BufferManager* bufferManager;
    WAL* wal;
    // This is the index of the last free byte to which we can write.
    PageCursor nextPosToWriteTo;
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
