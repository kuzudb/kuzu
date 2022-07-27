#pragma once

#include <iostream>

#include "src/common/include/configs.h"
#include "src/common/types/include/types.h"
#include "src/storage/buffer_manager/include/versioned_file_handle.h"
#include "src/storage/storage_structure/include/storage_structure.h"
#include "src/storage/wal/include/wal.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

class FileHandle;

static constexpr uint64_t NUM_PAGE_IDXS_PER_PIP =
    (DEFAULT_PAGE_SIZE - sizeof(page_idx_t)) / sizeof(page_idx_t);

/**
 * Header page of a disk array.
 */
struct DiskArrayHeader {
    // This constructor is needed when loading the database from file.
    DiskArrayHeader() : DiskArrayHeader(1){};

    explicit DiskArrayHeader(uint64_t elementSize);

    void saveToDisk(FileHandle& fileHandle, uint64_t headerPageIdx);

    void readFromFile(FileHandle& fileHandle, uint64_t headerPageIdx);

    // TODO(Semih): This is only for debugging purposes. Will be removed.
    void print();

    // We do not need to store numElementsPerPageLog2, elementPageOffsetMask, and numArrayPages or
    // save them on disk as they are functions of elementSize and numElements but we
    // nonetheless store them (and save them to disk) for simplicity.
    uint64_t elementSize;
    uint64_t numElementsPerPageLog2;
    uint64_t elementPageOffsetMask;
    uint64_t firstPIPPageIdx;
    uint64_t numElements;
    uint64_t numAPs;
};

struct PIP {
    PIP() : nextPipPageIdx{PAGE_IDX_MAX} {}

    // TODO(Semih): This is only for debugging purposes. Will be removed.
    void print();

    page_idx_t nextPipPageIdx;
    page_idx_t pageIdxs[NUM_PAGE_IDXS_PER_PIP];
};

struct PIPWrapper {
    PIPWrapper(FileHandle& fileHandle, page_idx_t pipPageIdx);

    PIPWrapper(page_idx_t pipPageIdx) : pipPageIdx(pipPageIdx) {}

    // TODO(Semih): This is only for debugging purposes. Will be removed.
    void print() {
        cout << "Printing PIPWrapper pipPageIdx: " << pipPageIdx << endl;
        pipContents.print();
    }

    page_idx_t pipPageIdx;
    PIP pipContents;
};

struct PIPUpdates {
    // updatedPipIdxs stores the idx's of existing PIPWrappers (not the physical pageIdx of those
    // PIPs, which are stored in the pipPageIdx field of PIPWrapper. These are used to replace the
    // PIPWrappers after commit quickly.
    unordered_set<uint64_t> updatedPipIdxs;
    vector<page_idx_t> pipPageIdxsOfinsertedPIPs;

    inline void clearNoLock() {
        updatedPipIdxs.clear();
        pipPageIdxsOfinsertedPIPs.clear();
    }
};

/**
 * DiskArray stores a disk-based array in a file. The array is broken down into a predefined and
 * stable header page, i.e., the header page of the array is always in a pre-allocated page in the
 * file. The header page contains the pointer to the first ``page indices page'' (pip). Each pip
 * stores a list of page indices that store the ``array pages''. Each pip also stores the pageIdx of
 * the next pip if one exists (or we use PAGE_IDX_MAX as null). Array pages store the actual data in
 * the array.
 *
 * Storage structures can use multiple disk arrays in a single file by giving each one a different
 * pre-allocated stable header pageIdxs.
 *
 * We clarify the following abbreviations and conventions in the variables used in these files:
 * <ul>
 *   <li> pip: Page Indices Page
 *   <li> pipIdx: logical index of a pip in DiskArray. For example a variable pipIdx we use with
 * value 5 indicates the 5th pip, not the physical disk pageIdx of where that PIP is stored.
 *   <li> pipPageIdx: the physical disk pageIdx of some PIP
 *   <li> AP: Array Page
 *   <li> apIdx: logical index of the array page in DiskArray. For example a variable apIdx with
 * value 5 indicates the 5th array page of the Disk Array (i.e., the physical offset of this would
 * correspond to the 5 element in the first PIP) not the physical disk pageIdx of an array page. <li
 *   <li> apPageIdx: the physicak disk pageIdx of some PIP.
 * </ul>
 */
template<typename U>
class BaseDiskArray {
public:
    // Used by builders
    BaseDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t elementSize);
    // Used when loading from file
    BaseDiskArray(
        FileHandle& fileHandle, page_idx_t headerPageIdx, BufferManager* bufferManager, WAL* wal);

    uint64_t getNumElements(TransactionType trxType = TransactionType::READ_ONLY);

    void updateForWriteTrx(uint64_t idx, U val);

    void pushBackForWriteTrx(U val);

    // TODO(Semih): This is only for debugging purposes. Will be removed.
    void print();

protected:
    uint64_t getNumElementsNoLock(TransactionType trxType);

    uint64_t getNumAPsNoLock(TransactionType trxType);

    void setNextPIPPageIDxOfPIPNoLock(DiskArrayHeader* updatedDiskArrayHeader,
        uint64_t pipIdxOfPreviousPIP, page_idx_t nextPIPPageIdx);

    // This function does division and mod and should not be used in performance critical code
    page_idx_t getAPPageIdxNoLock(
        page_idx_t apIdx, TransactionType trxType = TransactionType::READ_ONLY);

    // pipIdx is the idx of the pip, and not the physical pageIdx. This function assumes
    // that the caller has called hasPIPUpdatesNoLock and received true.
    page_idx_t getUpdatedPageIdxOfPipNoLock(uint64_t pipIdx);

    void clearUpdatedWALPageVersionAndRemovePageFromFrameIfNecessary(page_idx_t pageIdx);

    void checkpointOrRollbackInMemoryIfNecessaryNoLock(bool isCheckpoint);

private:
    bool hasPIPUpdatesNoLock(uint64_t pipIdx);

    uint64_t readUint64HeaderFieldNoLock(
        TransactionType trxType, std::function<uint64_t(DiskArrayHeader*)> readOp);

    // Sets the array page as updated, if the apIdx is < the "old version" of header.numAPs before
    // the udpates started (that's why we directly read header.numAPs).
    inline void setArrayPageUpdatedIfNecessaryNoLock(uint64_t apIdx) {
        if (apIdx < header.numAPs) {
            isArrayPageUpdated[apIdx] = true;
        }
    }

    // Returns the apPageIdx of the AP with idx apIdx and a bool indicating whether the apPageIdx is
    // a newly added page.
    pair<page_idx_t, bool> getAPPageIdxAndAddAPToPIPIfNecessaryForWriteTrxNoLock(
        DiskArrayHeader* updatedDiskArrayHeader, page_idx_t apIdx);

    void clearPIPUpdatesAndIsArrayPageUpdated(uint64_t numAPs);

public:
    DiskArrayHeader header;

protected:
    FileHandle& fileHandle;
    page_idx_t headerPageIdx;
    bool hasTransactionalUpdates;
    BufferManager* bufferManager;
    WAL* wal;
    vector<PIPWrapper> pips;
    PIPUpdates pipUpdates;
    shared_mutex diskArraySharedMtx;
    vector<bool> isArrayPageUpdated;
};

template<typename U>
class BaseInMemDiskArray : public BaseDiskArray<U> {
protected:
    BaseInMemDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t elementSize);
    BaseInMemDiskArray(
        FileHandle& fileHandle, page_idx_t headerPageIdx, BufferManager* bufferManager, WAL* wal);

public:
    // [] operator can be used to update elements, e.g., diskArray[5] = 4, when building an
    // InMemDiskArrayBuilder without transactional updates. This changes the contents directly in
    // memory and not on disk (nor on the wal).
    U& operator[](uint64_t idx);
    U get(uint64_t idx, TransactionType trxType = TransactionType::READ_ONLY);

    // TODO(Semih): This is only for debugging purposes. Will be removed.
    void print();

protected:
    uint64_t addInMemoryArrayPage() {
        inMemArrayPages.emplace_back(make_unique<U[]>(1ull << this->header.numElementsPerPageLog2));
        return inMemArrayPages.size() - 1;
    }

    void readArrayPageFromFile(uint64_t apIdx, page_idx_t apPageIdx);

    void addInMemoryArrayPageAndReadFromFile(page_idx_t apPageIdx);

protected:
    vector<unique_ptr<U[]>> inMemArrayPages;
};

/**
 * Stores an array of type U's page by page in memory, using OS memory and not the buffer manager.
 * Designed currently to be used by lists headers and metadata, where we want to avoid using
 * pins/unpins when accessing data through the buffer manager.
 */
template<typename T>
class InMemDiskArray : public BaseInMemDiskArray<T> {
public:
    InMemDiskArray(VersionedFileHandle& fileHandle, page_idx_t headerPageIdx,
        BufferManager* bufferManager, WAL* wal);

    void checkpointInMemoryIfNecessary();
    void rollbackInMemoryIfNecessary();

    inline VersionedFileHandle* getFileHandle() { return (VersionedFileHandle*)&this->fileHandle; }

private:
    void checkpointOrRollbackInMemoryIfNecessaryNoLock(bool isCheckpoint);
};

template<typename T>
class InMemDiskArrayBuilder : public BaseInMemDiskArray<T> {
public:
    InMemDiskArrayBuilder(FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t numElements);

    // This function is designed to be used during building of a disk array, i.e., during loading.
    // In particular, it changes the needed capacity non-transactionally, i.e., without writing
    // anything to the wal.
    void setNewNumElementsAndIncreaseCapacityIfNeeded(uint64_t newNumElements);

    void saveToDisk();

private:
    inline uint64_t getNumArrayPagesNeededForElements(uint64_t numElements) {
        return (numElements >> this->header.numElementsPerPageLog2) +
               ((numElements & this->header.elementPageOffsetMask) > 0 ? 1 : 0);
    }
    void addNewArrayPageForBuilding();

    void setNumElementsAndAllocateDiskAPsForBuilding(uint64_t newNumElements);
};

} // namespace storage
} // namespace graphflow
