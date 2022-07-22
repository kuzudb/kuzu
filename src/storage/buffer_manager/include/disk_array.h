#pragma once

#include <iostream>

#include "src/common/include/configs.h"
#include "src/common/types/include/types.h"

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
    uint64_t numArrayPages;
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
 */
class BaseDiskArray {
public:
    // Used by builders
    BaseDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t elementSize);
    // Used when loading from file
    BaseDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx);

    // This function does division and mod and should not be used in performance critical code
    page_idx_t getArrayFilePageIdx(page_idx_t arrayPageIdx);

    // TODO(Semih): This is only for debugging purposes. Will be removed.
    void print();

public:
    DiskArrayHeader header;

protected:
    FileHandle& fileHandle;
    page_idx_t headerPageIdx;
    vector<PIPWrapper> pips;
};

template<class U>
class BaseInMemDiskArray : public BaseDiskArray {
protected:
    BaseInMemDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t elementSize);
    BaseInMemDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx);

public:
    // [] operator can be used to update elements, e.g., diskArray[5] = 4, when building an
    // InMemDiskArrayBuilder without transactional updates. This changes the contents directly in
    // memory and not on disk (nor on the wal).
    U& operator[](uint64_t idx);

    // TODO(Semih): This is only for debugging purposes. Will be removed.
    void print();

protected:
    void addInMemoryPage() {
        dataPages.emplace_back(make_unique<U[]>(1ull << header.numElementsPerPageLog2));
    }

protected:
    vector<unique_ptr<U[]>> dataPages;
};

/**
 * Stores an array of type U's page by page in memory, using OS memory and not the buffer manager.
 * Designed currently to be used by lists headers and metadata, where we want to avoid using
 * pins/unpins when accessing data through the buffer manager.
 */
template<typename T>
class InMemDiskArray : public BaseInMemDiskArray<T> {
public:
    InMemDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx);
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

    void setNumElementsAndAllocateDiskArrayPagesForBuilding(uint64_t newNumElements);
};

} // namespace storage
} // namespace graphflow
