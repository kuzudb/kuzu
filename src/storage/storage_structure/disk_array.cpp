#include "src/storage/storage_structure/include/disk_array.h"

#include "src/common/include/utils.h"
#include "src/storage/buffer_manager/include/versioned_file_handle.h"

namespace graphflow {
namespace storage {

DiskArrayHeader::DiskArrayHeader(uint64_t elementSize)
    : elementSize{elementSize}, numElementsPerPageLog2{static_cast<uint64_t>(
                                    floor(log2(DEFAULT_PAGE_SIZE / elementSize)))},
      elementPageOffsetMask{BitmaskUtils::all1sMaskForLeastSignificantBits(numElementsPerPageLog2)},
      firstPIPPageIdx{PAGE_IDX_MAX}, numElements{0}, numAPs{0} {}

void DiskArrayHeader::saveToDisk(FileHandle& fileHandle, uint64_t headerPageIdx) {
    FileUtils::writeToFile(fileHandle.getFileInfo(), reinterpret_cast<uint8_t*>(this),
        sizeof(DiskArrayHeader), headerPageIdx * fileHandle.getPageSize());
}

void DiskArrayHeader::readFromFile(FileHandle& fileHandle, uint64_t headerPageIdx) {
    FileUtils::readFromFile(fileHandle.getFileInfo(), reinterpret_cast<uint8_t*>(this),
        sizeof(DiskArrayHeader), headerPageIdx * fileHandle.getPageSize());
}

PIPWrapper::PIPWrapper(FileHandle& fileHandle, page_idx_t pipPageIdx) : pipPageIdx(pipPageIdx) {
    fileHandle.readPage(reinterpret_cast<uint8_t*>(&pipContents), pipPageIdx);
}

template<typename U>
BaseDiskArray<U>::BaseDiskArray(
    FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t elementSize)
    : header{elementSize}, fileHandle(fileHandle), headerPageIdx(headerPageIdx),
      hasTransactionalUpdates{false}, bufferManager{nullptr}, wal{nullptr} {}

template<typename U>
BaseDiskArray<U>::BaseDiskArray(
    FileHandle& fileHandle, page_idx_t headerPageIdx, BufferManager* bufferManager, WAL* wal)
    : fileHandle(fileHandle), headerPageIdx(headerPageIdx), hasTransactionalUpdates{false},
      bufferManager{bufferManager}, wal{wal} {
    header.readFromFile(this->fileHandle, headerPageIdx);
    if (this->header.firstPIPPageIdx != PAGE_IDX_MAX) {
        pips.emplace_back(fileHandle, header.firstPIPPageIdx);
        while (pips[pips.size() - 1].pipContents.nextPipPageIdx != PAGE_IDX_MAX) {
            pips.emplace_back(fileHandle, pips[pips.size() - 1].pipContents.nextPipPageIdx);
        }
    }
}

template<typename U>
uint64_t BaseDiskArray<U>::getNumElements(TransactionType trxType) {
    shared_lock slock{diskArraySharedMtx};
    return getNumElementsNoLock(trxType);
}

template<typename U>
uint64_t BaseDiskArray<U>::getNumElementsNoLock(TransactionType trxType) {
    return readUint64HeaderFieldNoLock(trxType,
        [](DiskArrayHeader* diskArrayHeader) -> uint64_t { return diskArrayHeader->numElements; });
}

template<typename U>
void BaseDiskArray<U>::update(uint64_t idx, U val) {
    unique_lock xlock{diskArraySharedMtx};
    hasTransactionalUpdates = true;
    if (idx >= getNumElementsNoLock(TransactionType::WRITE)) {
        throw RuntimeException(StringUtils::string_format(
            "idx: %d of the DiskArray to be updated is >= numElements in DiskArray%d.", idx,
            getNumElementsNoLock(TransactionType::WRITE)));
    }
    page_idx_t apIdx = idx >> header.numElementsPerPageLog2;
    uint64_t offsetInAP = idx & header.elementPageOffsetMask;
    // TODO: We are currently supporting only DiskArrays that can grow in size and not
    // those that can shrink in size. That is why we can use
    // getAPPageIdxNoLock(apIdx) directly to compute the physical page Idx because
    // any apIdx is guaranteed to be either in an existing PIP or a new PIP we added,
    // which getAPPageIdxNoLock will correctly locate: this function simply searches an exising PIP
    // if apIdx < numAPs stored in "previous" PIP; otherwise one of the newly inserted PIPs
    // stored inpipPageIdxsOfinsertedPIPs. If within a single transaction we could grow or shrink,
    // then getAPPageIdxNoLock logic needs to change to give the same guarantee (e.g., an apIdx = 0,
    // may no longer to be guaranteed to be in pips[0].)
    page_idx_t apPageIdx = getAPPageIdxNoLock(apIdx);
    StorageStructureUtils::updatePage((VersionedFileHandle&)(fileHandle), apPageIdx,
        false /* not inserting a new page */, *bufferManager, *wal,
        [&offsetInAP, &val](uint8_t* frame) -> void { ((U*)frame)[offsetInAP] = val; });
}

template<typename U>
void BaseDiskArray<U>::pushBack(U val) {
    unique_lock xlock{diskArraySharedMtx};
    hasTransactionalUpdates = true;
    StorageStructureUtils::updatePage((VersionedFileHandle&)(fileHandle), headerPageIdx,
        false /* not inserting a new page */, *bufferManager, *wal,
        [this, &val](uint8_t* frame) -> void {
            auto updatedDiskArrayHeader = ((DiskArrayHeader*)frame);
            auto elementIdx = updatedDiskArrayHeader->numElements;
            page_idx_t apIdx = elementIdx >> updatedDiskArrayHeader->numElementsPerPageLog2;
            uint64_t offsetInAP = elementIdx & updatedDiskArrayHeader->elementPageOffsetMask;
            auto apPageIdxAndIsNewlyAdded = getAPPageIdxAndAddAPToPIPIfNecessaryForWriteTrxNoLock(
                (DiskArrayHeader*)frame, apIdx);
            // Now do the push back.
            StorageStructureUtils::updatePage((VersionedFileHandle&)(fileHandle),
                apPageIdxAndIsNewlyAdded.first, apPageIdxAndIsNewlyAdded.second, *bufferManager,
                *wal,
                [&offsetInAP, &val](uint8_t* frame) -> void { ((U*)frame)[offsetInAP] = val; });
            updatedDiskArrayHeader->numElements++;
        });
}

template<typename U>
uint64_t BaseDiskArray<U>::getNumAPsNoLock(TransactionType trxType) {
    return readUint64HeaderFieldNoLock(trxType,
        [](DiskArrayHeader* diskArrayHeader) -> uint64_t { return diskArrayHeader->numAPs; });
}

template<typename U>
void BaseDiskArray<U>::setNextPIPPageIDxOfPIPNoLock(DiskArrayHeader* updatedDiskArrayHeader,
    uint64_t pipIdxOfPreviousPIP, page_idx_t nextPIPPageIdx) {
    // This happens if the first pip is being inserted, in which case we need to change the header.
    if (pipIdxOfPreviousPIP == UINT64_MAX) {
        updatedDiskArrayHeader->firstPIPPageIdx = nextPIPPageIdx;
    } else {
        page_idx_t pipPageIdxOfPreviousPIP = getUpdatedPageIdxOfPipNoLock(pipIdxOfPreviousPIP);
        /*
         * Note that we can safely pass insertingNewPage argument here. There are two cases;
         * 1) if pipPageIdxOfPreviousPIP is a new PIP: in that case the previous caller
         * would have already created the WAL page for it, so this function is not creating
         * pipPageIdxOfPreviousPIP. 2) if pipPageIdxOfPreviousPIP is an existing PIP, in which
         * case again this function is not creating pipPageIdxOfPreviousPIP.
         */
        StorageStructureUtils::updatePage((VersionedFileHandle&)(fileHandle),
            pipPageIdxOfPreviousPIP, false /* not inserting a new page */, *bufferManager, *wal,
            [&nextPIPPageIdx](
                const uint8_t* frame) -> void { ((PIP*)frame)->nextPipPageIdx = nextPIPPageIdx; });
        // The above updatePage operation changes the "previousPIP" identified by
        // pipIdxOfPreviousPIP, so we put it to updatedPIPIdxs if it was a pip that already existed
        // before this transaction started. If pipIdxOfPreviousPIP >= pips.size() then it must
        // already be in pipUpdates.pipPageIdxsOfInsertedPIPs, so we do not need to insert it to
        // pipUpdates.pipPageIdxsOfInsertedPIPs (and hence there is no else to the below if).
        if (pipIdxOfPreviousPIP < pips.size()) {
            pipUpdates.updatedPipIdxs.insert(pipIdxOfPreviousPIP);
        }
    }
}

template<typename U>
page_idx_t BaseDiskArray<U>::getAPPageIdxNoLock(page_idx_t apIdx, TransactionType trxType) {
    auto pipIdxAndOffset = StorageUtils::getQuotientRemainder(apIdx, NUM_PAGE_IDXS_PER_PIP);
    uint64_t pipIdx = pipIdxAndOffset.first;
    uint64_t offsetInPIP = pipIdxAndOffset.second;
    if ((trxType == TransactionType::READ_ONLY) || !hasPIPUpdatesNoLock(pipIdx)) {
        return pips[pipIdx].pipContents.pageIdxs[offsetInPIP];
    } else {
        page_idx_t retVal;
        page_idx_t pageIdxOfUpdatedPip = getUpdatedPageIdxOfPipNoLock(pipIdx);
        StorageStructureUtils::readWALVersionOfPage((VersionedFileHandle&)(fileHandle),
            pageIdxOfUpdatedPip, *bufferManager, *wal,
            [&retVal, &offsetInPIP](
                const uint8_t* frame) -> void { retVal = ((PIP*)frame)->pageIdxs[offsetInPIP]; });
        return retVal;
    }
}

template<typename U>
page_idx_t BaseDiskArray<U>::getUpdatedPageIdxOfPipNoLock(uint64_t pipIdx) {
    if (pipIdx < pips.size()) {
        return pips[pipIdx].pipPageIdx;
    }
    return pipUpdates.pipPageIdxsOfInsertedPIPs[pipIdx - pips.size()];
}

template<typename U>
void BaseDiskArray<U>::clearWALPageVersionAndRemovePageFromFrameIfNecessary(page_idx_t pageIdx) {
    ((VersionedFileHandle&)this->fileHandle).clearWALPageVersionIfNecessary(pageIdx);
    bufferManager->removePageFromFrameIfNecessary(this->fileHandle, pageIdx);
}

template<typename U>
void BaseDiskArray<U>::checkpointOrRollbackInMemoryIfNecessaryNoLock(bool isCheckpoint) {
    if (!hasTransactionalUpdates) {
        return;
    }
    // Note: We update the header regardless (even if it has not changed). We can optimize this
    // by adding logic that keep track of whether the header has been updated.
    if (isCheckpoint) {
        header.readFromFile(this->fileHandle, headerPageIdx);
    }
    clearWALPageVersionAndRemovePageFromFrameIfNecessary(headerPageIdx);
    for (uint64_t pipIdxOfUpdatedPIP : pipUpdates.updatedPipIdxs) {
        // Note: This should not cause a memory leak because PIPWrapper is a struct. So we
        // should overwrite the previous PIPWrapper's memory.
        if (isCheckpoint) {
            pips[pipIdxOfUpdatedPIP] = PIPWrapper(fileHandle, pips[pipIdxOfUpdatedPIP].pipPageIdx);
        }
        clearWALPageVersionAndRemovePageFromFrameIfNecessary(pips[pipIdxOfUpdatedPIP].pipPageIdx);
    }

    for (page_idx_t pipPageIdxOfNewPIP : pipUpdates.pipPageIdxsOfInsertedPIPs) {
        if (isCheckpoint) {
            pips.emplace_back(fileHandle, pipPageIdxOfNewPIP);
        }
        clearWALPageVersionAndRemovePageFromFrameIfNecessary(pipPageIdxOfNewPIP);
        if (!isCheckpoint) {
            // These are newly inserted pages, so we can truncate the file handle.
            ((VersionedFileHandle&)this->fileHandle)
                .removePageIdxAndTruncateIfNecessary(pipPageIdxOfNewPIP);
        }
    }
    // Note that we already updated the header to its correct state above.
    pipUpdates.clear();
    hasTransactionalUpdates = false;
}

template<typename U>
bool BaseDiskArray<U>::hasPIPUpdatesNoLock(uint64_t pipIdx) {
    // This is a request to a pipIdx > pips.size(). Since pips.size() is the original number of pips
    // we started with before the write transaction is updated, we return true, i.e., this PIP is
    // an "updated" PIP and should be read from the WAL version.
    if (pipIdx >= pips.size()) {
        return true;
    }
    return pipUpdates.updatedPipIdxs.contains(pipIdx);
}

template<typename U>
uint64_t BaseDiskArray<U>::readUint64HeaderFieldNoLock(
    TransactionType trxType, std::function<uint64_t(DiskArrayHeader*)> readOp) {
    auto versionedFileHandle = (VersionedFileHandle*)(&fileHandle);
    if ((trxType == TransactionType::READ_ONLY) ||
        !versionedFileHandle->hasWALPageVersionNoPageLock(headerPageIdx)) {
        return readOp(&this->header);
    } else {
        uint64_t retVal;
        StorageStructureUtils::readWALVersionOfPage((VersionedFileHandle&)(fileHandle),
            headerPageIdx, *bufferManager, *wal, [&retVal, &readOp](uint8_t* frame) -> void {
                retVal = readOp((DiskArrayHeader*)frame);
            });
        return retVal;
    }
}

template<typename U>
pair<page_idx_t, bool> BaseDiskArray<U>::getAPPageIdxAndAddAPToPIPIfNecessaryForWriteTrxNoLock(
    DiskArrayHeader* updatedDiskArrayHeader, page_idx_t apIdx) {
    if (apIdx < updatedDiskArrayHeader->numAPs) {
        // If the apIdx of the array page is < updatedDiskArrayHeader->numAPs, we do not have to
        // add a new array page, so directly return the pageIdx of the apIdx.
        return make_pair(getAPPageIdxNoLock(apIdx, TransactionType::WRITE),
            false /* is not inserting a new ap page */);
    } else {
        // apIdx even if it's being inserted should never be > updatedDiskArrayHeader->numAPs.
        assert(apIdx == updatedDiskArrayHeader->numAPs);
        // We need to add a new AP. This may further cause a new pip to be inserted, which is
        // handled by the if/else-if/else branch below.
        auto& versionedFileHandle = (VersionedFileHandle&)this->fileHandle;
        page_idx_t newAPPageIdx = versionedFileHandle.addNewPage();
        // We need to create a new array page and then add its apPageIdx (newAPPageIdx variable) to
        // an appropriate PIP.
        auto pipIdxAndOffsetOfNewAP =
            StorageUtils::getQuotientRemainder(apIdx, NUM_PAGE_IDXS_PER_PIP);
        uint64_t pipIdx = pipIdxAndOffsetOfNewAP.first;
        uint64_t offsetOfNewAPInPIP = pipIdxAndOffsetOfNewAP.second;
        updatedDiskArrayHeader->numAPs++;
        page_idx_t pipPageIdx = PAGE_IDX_MAX;
        bool isInsertingANewPIPPage = false;
        if (pipIdx < pips.size()) {
            // We do not need to insert a new pip and we need to add newAPPageIdx to a PIP that
            // existed before this transaction started.
            pipUpdates.updatedPipIdxs.insert(pipIdx);
            pipPageIdx = pips[pipIdx].pipPageIdx;
        } else if ((pipIdx - pips.size()) < pipUpdates.pipPageIdxsOfInsertedPIPs.size()) {
            // We do not need to insert a new PIP and we need to add newAPPageIdx to a new PIP that
            // already got created after this transaction started.
            pipPageIdx = pipUpdates.pipPageIdxsOfInsertedPIPs[pipIdx - pips.size()];
        } else {
            // We need to create a new PIP and make the previous PIP (or the header) point to it.
            isInsertingANewPIPPage = true;
            pipPageIdx = versionedFileHandle.addNewPage();
            pipUpdates.pipPageIdxsOfInsertedPIPs.push_back(pipPageIdx);
            uint64_t pipIdxOfPreviousPIP = pipIdx - 1;
            setNextPIPPageIDxOfPIPNoLock(updatedDiskArrayHeader, pipIdxOfPreviousPIP, pipPageIdx);
        }
        // Finally we update the PIP page (possibly newly created) and add newAPPageIdx into it.
        StorageStructureUtils::updatePage((VersionedFileHandle&)(fileHandle), pipPageIdx,
            isInsertingANewPIPPage, *bufferManager, *wal,
            [&isInsertingANewPIPPage, &newAPPageIdx, &offsetOfNewAPInPIP](const uint8_t* frame) -> void {
                if (isInsertingANewPIPPage) {
                    ((PIP*)frame)->nextPipPageIdx = PAGE_IDX_MAX;
                }
                ((PIP*)frame)->pageIdxs[offsetOfNewAPInPIP] = newAPPageIdx;
            });
        return make_pair(newAPPageIdx, true /* inserting a new ap page */);
    }
}

template<typename U>
BaseInMemDiskArray<U>::BaseInMemDiskArray(
    FileHandle& fileHandle, page_idx_t headerPageIdx, BufferManager* bufferManager, WAL* wal)
    : BaseDiskArray<U>(fileHandle, headerPageIdx, bufferManager, wal) {
    for (page_idx_t apIdx = 0; apIdx < this->header.numAPs; ++apIdx) {
        addInMemoryArrayPageAndReadFromFile(this->getAPPageIdxNoLock(apIdx));
    }
}

template<typename U>
BaseInMemDiskArray<U>::BaseInMemDiskArray(
    FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t elementSize)
    : BaseDiskArray<U>(fileHandle, headerPageIdx, elementSize) {}

// [] operator to be used when building an InMemDiskArrayBuilder without transactional updates.
// This changes the contents directly in memory and not on disk (nor on the wal)
template<typename U>
U& BaseInMemDiskArray<U>::operator[](uint64_t idx) {
    page_idx_t apIdx = idx >> this->header.numElementsPerPageLog2;
    uint64_t offsetInArrayPage = idx & this->header.elementPageOffsetMask;
    assert(apIdx < this->header.numAPs);
    return inMemArrayPages[apIdx][offsetInArrayPage];
}

template<typename U>
U BaseInMemDiskArray<U>::get(uint64_t idx, TransactionType trxType) {
    shared_lock slock(this->diskArraySharedMtx);
    page_idx_t apIdx = idx >> this->header.numElementsPerPageLog2;
    uint64_t offsetInAP = idx & this->header.elementPageOffsetMask;
    switch (trxType) {
    case READ_ONLY: {
        if (idx >= this->header.numElements) {
            throw RuntimeException("idx: " + to_string(idx) +
                                   " of the element in DiskArray is >= this->header.numElements: " +
                                   to_string(this->header.numElements) + " for read trx.");
        }
        return inMemArrayPages[apIdx][offsetInAP];
    }
    case WRITE: {
        if (idx >= this->getNumElementsNoLock(TransactionType::WRITE)) {
            throw RuntimeException("idx: " + to_string(idx) +
                                   " of the element in DiskArray is >= this->header.numElements: " +
                                   to_string(this->header.numElements) + " for write trx.");
        }
        auto& versionedFileHandle = (VersionedFileHandle&)this->fileHandle;
        page_idx_t apPageIdx = this->getAPPageIdxNoLock(apIdx, TransactionType::WRITE);
        if (versionedFileHandle.hasWALPageVersionNoPageLock(apPageIdx)) {
            // apPageIdx has an updated version, so we read from the WAL version of apPageIdx.
            U retVal;
            StorageStructureUtils::readWALVersionOfPage(versionedFileHandle, apPageIdx,
                *this->bufferManager, *this->wal, [&retVal, &offsetInAP](const uint8_t* frame) -> void {
                    retVal = ((U*)frame)[offsetInAP];
                });
            return retVal;
        } else {
            // apPageIdx does not have an updated version, so we directly read from the
            // inMemArrayPages of the apIdx.
            return inMemArrayPages[apIdx][offsetInAP];
        }
    }
    default: {
        throw RuntimeException(
            "Unrecognized TransactionType: " + to_string(trxType) + ". This should never happen.");
    }
    }
}
template<typename U>
void BaseInMemDiskArray<U>::addInMemoryArrayPageAndReadFromFile(page_idx_t apPageIdx) {
    uint64_t apIdx = this->addInMemoryArrayPage();
    readArrayPageFromFile(apIdx, apPageIdx);
}

template<typename U>
void BaseInMemDiskArray<U>::readArrayPageFromFile(uint64_t apIdx, page_idx_t apPageIdx) {
    this->fileHandle.readPage(
        reinterpret_cast<uint8_t*>(this->inMemArrayPages[apIdx].get()), apPageIdx);
}

template<typename T>
InMemDiskArray<T>::InMemDiskArray(VersionedFileHandle& fileHandle, page_idx_t headerPageIdx,
    BufferManager* bufferManager, WAL* wal)
    : BaseInMemDiskArray<T>(fileHandle, headerPageIdx, bufferManager, wal) {}

template<typename T>
void InMemDiskArray<T>::checkpointInMemoryIfNecessary() {
    unique_lock xlock{this->diskArraySharedMtx};
    checkpointOrRollbackInMemoryIfNecessaryNoLock(true /* is checkpoint */);
}

template<typename T>
void InMemDiskArray<T>::rollbackInMemoryIfNecessary() {
    unique_lock xlock{this->diskArraySharedMtx};
    InMemDiskArray<T>::checkpointOrRollbackInMemoryIfNecessaryNoLock(false /* is rollback */);
}

template<typename T>
void InMemDiskArray<T>::checkpointOrRollbackInMemoryIfNecessaryNoLock(bool isCheckpoint) {
    if (!this->hasTransactionalUpdates) {
        return;
    }
    uint64_t numOldAPs = this->getNumAPsNoLock(TransactionType::READ_ONLY);
    for (uint64_t apIdx = 0; apIdx < numOldAPs; ++apIdx) {
        uint64_t apPageIdx = this->getAPPageIdxNoLock(apIdx, TransactionType::READ_ONLY);
        if (((VersionedFileHandle&)this->fileHandle).hasWALPageVersionNoPageLock(apPageIdx)) {
            // Note we can directly read the new image from disk because the WALReplayer checkpoints
            // the disk image of the page before calling
            // InMemDiskArray::checkpointInMemoryIfNecessary.
            if (isCheckpoint) {
                this->readArrayPageFromFile(apIdx, apPageIdx);
            }
            this->clearWALPageVersionAndRemovePageFromFrameIfNecessary(apPageIdx);
        }
    }
    uint64_t newNumAPs = this->getNumAPsNoLock(TransactionType::WRITE);
    // When rolling back, unlike removing new PIPs in
    // BaseDiskArray::checkpointOrRollbackInMemoryIfNecessaryNoLock when rolling back, we cannot
    // directly truncate each page. Instead we need to keep track of the minimum apPageIdx we
    // saw that we want to truncate to first. Then we call
    // BaseDiskArray::checkpointOrRollbackInMemoryIfNecessaryNoLock, which can do its own
    // truncation due to newly added PIPs. Then finally we truncate. The reason is this: suppose
    // we added a new apIdx=1 with pageIdx 20 in the fileHandle, which suppose caused a new PIP
    // to be inserted with pageIdx 21, and we further added one more new apIdx=2 with
    // pageIdx 22. Now this function will loop through the newly added apIdxs, so apIdx=1 and 2
    // in that order. If we directly truncate to the pageIdx of apIdx=1, which is 20, then we
    // will remove 21 and 22. But then we will loop through apIdx=2 and we need to convert it to
    // its pageIdx to clear its updated WAL version. But that requires reading the newly added
    // PIP's WAL version, which had pageIdx 22 but no longer exists. That would lead to a seg
    // fault somewhere. So we do not truncate these in order not to accidentally remove newly
    // added PIPs, which we would need if we kept calling removePageIdxAndTruncateIfNecessary
    // for each newly added array pages.
    page_idx_t minNewAPPageIdxToTruncateTo = PAGE_IDX_MAX;
    for (uint64_t apIdx = this->header.numAPs; apIdx < newNumAPs; apIdx++) {
        page_idx_t apPageIdx = this->getAPPageIdxNoLock(apIdx, TransactionType::WRITE);
        if (isCheckpoint) {
            this->addInMemoryArrayPageAndReadFromFile(apPageIdx);
        }
        this->clearWALPageVersionAndRemovePageFromFrameIfNecessary(apPageIdx);
        if (!isCheckpoint) {
            minNewAPPageIdxToTruncateTo = min(minNewAPPageIdxToTruncateTo, apPageIdx);
        }
    }
    // TODO(Semih): Currently we do not support truncating DiskArrays. When we support that, we
    // need to implement the logic to truncate InMemArrayPages as well.
    // Note that the base class call sets hasTransactionalUpdates to false.
    if (isCheckpoint) {
        BaseDiskArray<T>::checkpointOrRollbackInMemoryIfNecessaryNoLock(true /* is checkpoint */);
    } else {
        BaseDiskArray<T>::checkpointOrRollbackInMemoryIfNecessaryNoLock(false /* is rollback */);
        ((VersionedFileHandle&)this->fileHandle)
            .removePageIdxAndTruncateIfNecessary(minNewAPPageIdxToTruncateTo);
    }
}

template<typename T>
InMemDiskArrayBuilder<T>::InMemDiskArrayBuilder(
    FileHandle& fileHandle, page_idx_t headerPageIdx, uint64_t numElements)
    : BaseInMemDiskArray<T>(fileHandle, headerPageIdx, sizeof(T)) {
    setNumElementsAndAllocateDiskAPsForBuilding(numElements);
    for (uint64_t i = 0; i < this->header.numAPs; ++i) {
        this->addInMemoryArrayPage();
    }
}

template<typename T>
void InMemDiskArrayBuilder<T>::setNewNumElementsAndIncreaseCapacityIfNeeded(
    uint64_t newNumElements) {
    uint64_t oldNumAPs = this->header.numAPs;
    setNumElementsAndAllocateDiskAPsForBuilding(newNumElements);
    uint64_t newNumAPs = this->header.numAPs;
    for (int i = oldNumAPs; i < newNumAPs; ++i) {
        this->addInMemoryArrayPage();
    }
}

template<typename T>
void InMemDiskArrayBuilder<T>::saveToDisk() {
    // save the header and pips.
    this->header.saveToDisk(this->fileHandle, this->headerPageIdx);
    for (int i = 0; i < this->pips.size(); ++i) {
        this->fileHandle.writePage(
            reinterpret_cast<uint8_t*>(&this->pips[i].pipContents), this->pips[i].pipPageIdx);
    }
    // Save array pages
    for (page_idx_t apIdx = 0; apIdx < this->header.numAPs; ++apIdx) {
        this->fileHandle.writePage(reinterpret_cast<uint8_t*>(this->inMemArrayPages[apIdx].get()),
            this->getAPPageIdxNoLock(apIdx));
    }
}

template<typename T>
void InMemDiskArrayBuilder<T>::addNewArrayPageForBuilding() {
    uint64_t arrayPageIdx = this->fileHandle.addNewPage();
    // The idx of the next array page will be exactly header.numArrayPages. That is why we first
    // find the pipIdx and offset in the PIP of the array page before incrementing
    // header.numArrayPages by 1.
    auto pipIdxAndOffset =
        StorageUtils::getQuotientRemainder(this->header.numAPs, NUM_PAGE_IDXS_PER_PIP);
    this->header.numAPs++;
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

template<typename T>
void InMemDiskArrayBuilder<T>::setNumElementsAndAllocateDiskAPsForBuilding(
    uint64_t newNumElements) {
    uint64_t oldNumArrayPages = this->header.numAPs;
    uint64_t newNumArrayPages = getNumArrayPagesNeededForElements(newNumElements);
    for (int i = oldNumArrayPages; i < newNumArrayPages; ++i) {
        addNewArrayPageForBuilding();
    }
    this->header.numElements = newNumElements;
    this->header.numAPs = newNumArrayPages;
}

template class BaseDiskArray<uint32_t>;
template class BaseInMemDiskArray<uint32_t>;
template class InMemDiskArrayBuilder<uint32_t>;
template class InMemDiskArray<uint32_t>;
} // namespace storage
} // namespace graphflow
