#include "src/storage/buffer_manager/include/disk_array.h"

#include "src/common/include/utils.h"
#include "src/storage/buffer_manager/include/versioned_file_handle.h"

namespace graphflow {
namespace storage {

DiskArrayHeader::DiskArrayHeader(uint64_t elementSize)
    : elementSize{elementSize}, numElementsPerPageLog2{static_cast<uint64_t>(
                                    floor(log2(DEFAULT_PAGE_SIZE / elementSize)))},
      elementPageOffsetMask{BitmaskUtils::all1sMaskForLeastSignificantBits(numElementsPerPageLog2)},
      firstPIPPageIdx{PAGE_IDX_MAX}, numElements{0}, numArrayPages{0} {}

void DiskArrayHeader::saveToDisk(FileHandle& fileHandle, uint64_t headerPageIdx) {
    FileUtils::writeToFile(fileHandle.getFileInfo(), reinterpret_cast<uint8_t*>(this),
        sizeof(DiskArrayHeader), headerPageIdx * fileHandle.getPageSize());
}

void DiskArrayHeader::readFromFile(FileHandle& fileHandle, uint64_t headerPageIdx) {
    FileUtils::readFromFile(fileHandle.getFileInfo(), reinterpret_cast<uint8_t*>(this),
        sizeof(DiskArrayHeader), headerPageIdx * fileHandle.getPageSize());
}

void DiskArrayHeader::print() {
    cout << "firstPIPPageIdx: " << firstPIPPageIdx << endl;
    cout << "elementSize: " << elementSize << endl;
    cout << "numElements: " << numElements << endl;
    cout << "numElementsPerPageLog2: " << numElementsPerPageLog2 << endl;
    cout << "elementPageOffsetMask: " << elementPageOffsetMask << endl;
    cout << "numArrayPages: " << numArrayPages << endl;
}

void PIP::print() {
    cout << "nextPipPageIdx: " << nextPipPageIdx << endl;
    cout << "pageIdxs:";
    for (int i = 0; i < NUM_PAGE_IDXS_PER_PIP; ++i) {
        cout << " " << pageIdxs[i];
    }
    cout << endl;
}

PIPWrapper::PIPWrapper(FileHandle& fileHandle, page_idx_t pipPageIdx) : pipPageIdx(pipPageIdx) {
    fileHandle.readPage(reinterpret_cast<uint8_t*>(&pipContents), pipPageIdx);
}

BaseDiskArray::BaseDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t elementSize)
    : header{elementSize}, fileHandle(fileHandle), headerPageIdx(headerPageIdx) {}

BaseDiskArray::BaseDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx)
    : fileHandle(fileHandle), headerPageIdx(headerPageIdx) {
    header.readFromFile(this->fileHandle, headerPageIdx);
    if (header.firstPIPPageIdx != PAGE_IDX_MAX) {
        pips.emplace_back(fileHandle, header.firstPIPPageIdx);
        while (pips[pips.size() - 1].pipContents.nextPipPageIdx != PAGE_IDX_MAX) {
            pips.emplace_back(fileHandle, pips[pips.size() - 1].pipContents.nextPipPageIdx);
        }
    }
}

template<class T>
void InMemDiskArrayBuilder<T>::setNumElementsAndAllocateDiskArrayPagesForBuilding(
    uint64_t newNumElements) {
    uint64_t oldNumArrayPages = this->header.numArrayPages;
    uint64_t newNumArrayPages = getNumArrayPagesNeededForElements(newNumElements);
    for (int i = oldNumArrayPages; i < newNumArrayPages; ++i) {
        addNewArrayPageForBuilding();
    }
    this->header.numElements = newNumElements;
    this->header.numArrayPages = newNumArrayPages;
}

template<class T>
void InMemDiskArrayBuilder<T>::addNewArrayPageForBuilding() {
    uint64_t arrayPageIdx = this->fileHandle.addNewPage();
    // The idx of the next array page will be exactly header.numArrayPages. That is why we first
    // find the pipIdx and offset in the pip of the array page before incrementing
    // header.numArrayPages by 1.
    auto pipIdxAndOffset =
        StorageUtils::getQuotientRemainder(this->header.numArrayPages, NUM_PAGE_IDXS_PER_PIP);
    this->header.numArrayPages++;
    uint64_t pipIdx = pipIdxAndOffset.first;
    if (pipIdx == this->pips.size()) {
        uint64_t pipPageIdx = this->fileHandle.addNewPage();
        this->pips.emplace_back(pipPageIdx);
        if (pipIdx == 0) {
            this->header.firstPIPPageIdx = pipPageIdx;
        } else {
            this->pips[pipIdx - 1].pipContents.nextPipPageIdx = pipPageIdx;
        }
    }
    this->pips[pipIdx].pipContents.pageIdxs[pipIdxAndOffset.second] = arrayPageIdx;
}

page_idx_t BaseDiskArray::getArrayFilePageIdx(page_idx_t arrayPageIdx) {
    assert(arrayPageIdx < header.numArrayPages);
    auto pipIdxAndOffset = StorageUtils::getQuotientRemainder(arrayPageIdx, NUM_PAGE_IDXS_PER_PIP);
    return pips[pipIdxAndOffset.first].pipContents.pageIdxs[pipIdxAndOffset.second];
}

void BaseDiskArray::print() {
    cout << "Printing BaseDisk Array" << endl;
    cout << "headerPageIdx: " << headerPageIdx << endl;
    header.print();
    for (int i = 0; i < pips.size(); ++i) {
        pips[i].print();
    }
}

template<class U>
BaseInMemDiskArray<U>::BaseInMemDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx)
    : BaseDiskArray(fileHandle, headerPageIdx) {
    for (page_idx_t arrayPageIdx = 0; arrayPageIdx < header.numArrayPages; ++arrayPageIdx) {
        addInMemoryPage();
        fileHandle.readPage(reinterpret_cast<uint8_t*>(dataPages[arrayPageIdx].get()),
            getArrayFilePageIdx(arrayPageIdx));
    }
}
template<class U>
BaseInMemDiskArray<U>::BaseInMemDiskArray(
    FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t elementSize)
    : BaseDiskArray(fileHandle, headerPageIdx, elementSize) {}

// [] operator to be used when building an InMemDiskArrayBuilder without transactional updates. This
// changes the contents directly in memory and not on disk (nor on the wal)
template<class U>
U& BaseInMemDiskArray<U>::operator[](uint64_t idx) {
    page_idx_t arrayPageIdx = idx >> header.numElementsPerPageLog2;
    uint64_t arrayPageOffset = idx & header.elementPageOffsetMask;
    assert(arrayPageIdx < header.numArrayPages);
    return dataPages[arrayPageIdx][arrayPageOffset];
}

template<class T>
InMemDiskArray<T>::InMemDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx)
    : BaseInMemDiskArray<T>(fileHandle, headerPageIdx) {}

template<class T>
InMemDiskArrayBuilder<T>::InMemDiskArrayBuilder(
    FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t numElements)
    : BaseInMemDiskArray<T>(fileHandle, headerPageIdx, sizeof(T)) {
    setNumElementsAndAllocateDiskArrayPagesForBuilding(numElements);
    for (uint64_t i = 0; i < this->header.numArrayPages; ++i) {
        this->addInMemoryPage();
    }
}

template<class T>
void InMemDiskArrayBuilder<T>::saveToDisk() {
    // save the header and pips.
    this->header.saveToDisk(this->fileHandle, this->headerPageIdx);
    for (int i = 0; i < this->pips.size(); ++i) {
        this->fileHandle.writePage(
            reinterpret_cast<uint8_t*>(&this->pips[i].pipContents), this->pips[i].pipPageIdx);
    }
    // Save array pages
    for (page_idx_t arrayPageIdx = 0; arrayPageIdx < this->header.numArrayPages; ++arrayPageIdx) {
        this->fileHandle.writePage(reinterpret_cast<uint8_t*>(this->dataPages[arrayPageIdx].get()),
            this->getArrayFilePageIdx(arrayPageIdx));
    }
}

template<class T>
void InMemDiskArrayBuilder<T>::setNewNumElementsAndIncreaseCapacityIfNeeded(
    uint64_t newNumElements) {
    uint64_t oldNumArrayPages = this->header.numArrayPages;
    setNumElementsAndAllocateDiskArrayPagesForBuilding(newNumElements);
    uint64_t newNumArrayPages = this->header.numArrayPages;
    for (int i = oldNumArrayPages; i < newNumArrayPages; ++i) {
        this->addInMemoryPage();
    }
}

template<class U>
void BaseInMemDiskArray<U>::print() {
    BaseDiskArray::print();
    for (page_idx_t arrayPageIdx = 0; arrayPageIdx < header.numArrayPages; ++arrayPageIdx) {
        auto numElementsPerPage = 1 << header.numElementsPerPageLog2;
        cout << "array page " << arrayPageIdx
             << " (file pageIdx: " << getArrayFilePageIdx(arrayPageIdx) << ") contents: ";
        for (int j = 0; j < numElementsPerPage; ++j) {
            cout << " " << dataPages[arrayPageIdx][j];
        }
        cout << endl;
        dataPages.emplace_back(make_unique<U[]>(1 << header.numElementsPerPageLog2));
    }
}
template class BaseInMemDiskArray<uint32_t>;
template class InMemDiskArrayBuilder<uint32_t>;
template class InMemDiskArray<uint32_t>;
} // namespace storage
} // namespace graphflow
