#include "include/disk_array.h"

namespace graphflow {
namespace storage {

DiskArrayHeader::DiskArrayHeader(
    uint64_t elementSize, uint64_t numElements, uint64_t firstPIPPageIdx)
    : elementSize{elementSize}, numElements{numElements}, firstPIPPageIdx{firstPIPPageIdx} {
    numElementsPerPageLog2 = static_cast<uint64_t>(floor(log2(DEFAULT_PAGE_SIZE / elementSize)));
    elementPageOffsetMask = StorageUtils::all1sMaskForLeastSignificantBits(numElementsPerPageLog2);
    numArrayPages = (numElements >> numElementsPerPageLog2) +
                    ((numElements & elementPageOffsetMask) > 0 ? 1 : 0);
}

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

BaseDiskArray::BaseDiskArray(shared_ptr<FileHandle> fileHandle, page_idx_t headerPageIdx,
    uint64_t elementSize, uint64_t numElements)
    : header(elementSize, numElements, fileHandle->addNewPage() /* first pip pageIdx */),
      fileHandle(fileHandle), headerPageIdx(headerPageIdx) {
    assert(!fileHandle->isLargePaged());
    uint64_t numArrayPagesLeft = header.numArrayPages;
    uint64_t pipIdx = 0;
    while (numArrayPagesLeft > 0) {
        uint64_t pipPageIdx = (pipIdx == 0) ? header.firstPIPPageIdx : fileHandle->addNewPage();
        pips.emplace_back(pipPageIdx);
        if (pipIdx > 0) {
            pips[pipIdx - 1].pipContents.nextPipPageIdx = pipPageIdx;
        }
        pipIdx++;
        numArrayPagesLeft -= min(numArrayPagesLeft, NUM_PAGE_IDXS_PER_PIP);
    }
    for (int i = 0; i < header.numArrayPages; ++i) {
        uint64_t pageIdx = fileHandle->addNewPage();
        auto pipIdxAndOffset = StorageUtils::getQuotientRemainder(i, NUM_PAGE_IDXS_PER_PIP);
        pips[pipIdxAndOffset.first].pipContents.pageIdxs[pipIdxAndOffset.second] = pageIdx;
    }
}

BaseDiskArray::BaseDiskArray(shared_ptr<FileHandle> fileHandle, page_idx_t headerPageIdx)
    : fileHandle(fileHandle), headerPageIdx(headerPageIdx) {
    header.readFromFile(*this->fileHandle, headerPageIdx);
    pips.emplace_back(fileHandle, header.firstPIPPageIdx);
    while (pips[pips.size() - 1].pipContents.nextPipPageIdx != PAGE_IDX_MAX) {
        pips.emplace_back(fileHandle, pips[pips.size() - 1].pipContents.nextPipPageIdx);
    }
}

page_idx_t BaseDiskArray::getArrayFilePageIdx(page_idx_t arrayPageIdx) {
    assert(arrayPageIdx < header.numArrayPages);
    auto pipIdxAndOffset = StorageUtils::getQuotientRemainder(arrayPageIdx, NUM_PAGE_IDXS_PER_PIP);
    return pips[pipIdxAndOffset.first].pipContents.pageIdxs[pipIdxAndOffset.second];
}

void BaseDiskArray::saveToDisk() {
    header.saveToDisk(*fileHandle, headerPageIdx);
    for (int i = 0; i < pips.size(); ++i) {
        fileHandle->writePage(reinterpret_cast<uint8_t*>(&pips[i].pipContents), pips[i].pipPageIdx);
    }
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
InMemDiskArray<U>::InMemDiskArray(
    shared_ptr<FileHandle> fileHandle, page_idx_t headerPageIdx, uint64_t numElements)
    : BaseDiskArray(fileHandle, headerPageIdx, sizeof(U) /* elementSize */, numElements) {
    for (uint64_t i = 0; i < header.numArrayPages; ++i) {
        dataPages.emplace_back(make_unique<U[]>(1ull << header.numElementsPerPageLog2));
    }
}

template<class U>
InMemDiskArray<U>::InMemDiskArray(shared_ptr<FileHandle> fileHandle, page_idx_t headerPageIdx)
    : BaseDiskArray(fileHandle, headerPageIdx) {
    for (page_idx_t arrayPageIdx = 0; arrayPageIdx < header.numArrayPages; ++arrayPageIdx) {
        dataPages.emplace_back(make_unique<U[]>(1ull << header.numElementsPerPageLog2));
        fileHandle->readPage(reinterpret_cast<uint8_t*>(dataPages[arrayPageIdx].get()),
            getArrayFilePageIdx(arrayPageIdx));
    }
}

// [] operator to be used when building an InMemDiskArray without transactional updates. This
// changes the contents directly in memory and not on disk (nor on the wal)
template<class U>
U& InMemDiskArray<U>::operator[](uint64_t idx) {
    page_idx_t arrayPageIdx = idx >> header.numElementsPerPageLog2;
    uint64_t arrayPageOffset = idx & header.elementPageOffsetMask;
    assert(arrayPageIdx < header.numArrayPages);
    return dataPages[arrayPageIdx][arrayPageOffset];
}

template<class U>
void InMemDiskArray<U>::saveToDisk() {
    BaseDiskArray::saveToDisk();
    for (page_idx_t arrayPageIdx = 0; arrayPageIdx < header.numArrayPages; ++arrayPageIdx) {
        fileHandle->writePage(reinterpret_cast<uint8_t*>(dataPages[arrayPageIdx].get()),
            getArrayFilePageIdx(arrayPageIdx));
    }
}

template<class U>
void InMemDiskArray<U>::print() {
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

template class InMemDiskArray<list_header_t>;

} // namespace storage
} // namespace graphflow
