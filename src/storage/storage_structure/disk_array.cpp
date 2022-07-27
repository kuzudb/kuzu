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

void DiskArrayHeader::print() {
    cout << "firstPIPPageIdx: " << firstPIPPageIdx << endl;
    cout << "elementSize: " << elementSize << endl;
    cout << "numElements: " << numElements << endl;
    cout << "numElementsPerPageLog2: " << numElementsPerPageLog2 << endl;
    cout << "elementPageOffsetMask: " << elementPageOffsetMask << endl;
    cout << "numAPs: " << numAPs << endl;
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
    isArrayPageUpdated.resize(header.numAPs, false /* initialize all pages to not updated */);
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
void BaseDiskArray<U>::updateForWriteTrx(uint64_t idx, U val) {
    unique_lock xlock{diskArraySharedMtx};
    hasTransactionalUpdates = true;
    if (idx >= getNumElementsNoLock(TransactionType::WRITE)) {
        throw RuntimeException(StringUtils::string_format(
            "idx: %d of the DiskArray to be updated is < numElements in DiskArray%d.", idx,
            getNumElementsNoLock(TransactionType::WRITE)));
    }
    page_idx_t apIdx = idx >> header.numElementsPerPageLog2;
    uint64_t offsetInAP = idx & header.elementPageOffsetMask;
    // TODO: We are currently supporting only DiskArrays that can grow in size and not
    // those that can shrink in size. That is why we can use
    // getAPPageIdxNoLock(apIdx) directly to compute the physical page Idx.
    page_idx_t apPageIdx = getAPPageIdxNoLock(apIdx);
    setArrayPageUpdatedIfNecessaryNoLock(apIdx);
    StorageStructureUtils::updatePageTransactionally((VersionedFileHandle&)(fileHandle), apPageIdx,
        false /* not inserting a new page */, *bufferManager, *wal,
        [&offsetInAP, &val](uint8_t* frame) -> void { ((U*)frame)[offsetInAP] = val; });
}

template<typename U>
void BaseDiskArray<U>::pushBackForWriteTrx(U val) {
    unique_lock xlock{diskArraySharedMtx};
    hasTransactionalUpdates = true;
    StorageStructureUtils::updatePageTransactionally((VersionedFileHandle&)(fileHandle),
        headerPageIdx, false /* not inserting a new page */, *bufferManager, *wal,
        [this, &val](uint8_t* frame) -> void {
            auto updatedDiskArrayHeader = ((DiskArrayHeader*)frame);
            auto elementIdx = updatedDiskArrayHeader->numElements;
            page_idx_t apIdx = elementIdx >> updatedDiskArrayHeader->numElementsPerPageLog2;
            uint64_t offsetInAP = elementIdx & updatedDiskArrayHeader->elementPageOffsetMask;
            auto apPageIdxAndIsNewlyAdded = getAPPageIdxAndAddAPToPIPIfNecessaryForWriteTrxNoLock(
                (DiskArrayHeader*)frame, apIdx);
            setArrayPageUpdatedIfNecessaryNoLock(apIdx);
            // Now do the push back.
            StorageStructureUtils::updatePageTransactionally((VersionedFileHandle&)(fileHandle),
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
        cout << "Setting firstPIPPageIdx of header to nextPIPPageIdx: " << nextPIPPageIdx << endl;
        updatedDiskArrayHeader->firstPIPPageIdx = nextPIPPageIdx;
    } else {
        page_idx_t pipPageIdxOfPreviousPIP = getUpdatedPageIdxOfPipNoLock(pipIdxOfPreviousPIP);
        /*
         * Note that pipPageIdxOfPreviousPIP may be a new page but in that case the previous caller
         *  would have already created the WAL page for it, so we can safely pass false for
         *  insertionNewPage argument here. The reason this assumption holds is that we assume the
         *  pipIdxOfPreviousPIP argument is pipIdx of a PIP that already exists and the caller has
         *  created a new PIP nextPIPPageIdx and wants pipIdxOfPreviousPIP to point to
         *  nextPIPPageIdx. If pipIdxOfPreviousPIP was an existing PIP prior to the transaction,
         * then it is possible that the WAL page has not been created for it and this function will
         *  create it correctly. This case would trigger if prior to the transaction
         *  pipIdxOfPreviousPIP was completely full and the caller pushed back 1 new element into
         * the DiskArray.
         */
        cout << "Setting the nextPipPageIdx of pipIdxOfPreviousPIP: " << pipIdxOfPreviousPIP
             << " to: " << nextPIPPageIdx << endl;
        StorageStructureUtils::updatePageTransactionally((VersionedFileHandle&)(fileHandle),
            pipPageIdxOfPreviousPIP, false /* not inserting a new page */, *bufferManager, *wal,
            [&nextPIPPageIdx](
                uint8_t* frame) -> void { ((PIP*)frame)->nextPipPageIdx = nextPIPPageIdx; });
        // This operation changes the "previousPIP" identified by pipIdxOfPreviousPIP, so we put
        // it to updatedPIPIdxs if it was a pip that already existed before this transaction
        // started. If pipIdxOfPreviousPIP >= pips.size() then it must already be in
        // pipUpdates.pipPageIdxsOfinsertedPIPs, so we do not need to insert it to
        // pipUpdates.pipPageIdxsOfinsertedPIPs.
        if (pipIdxOfPreviousPIP < pips.size()) {
            pipUpdates.updatedPipIdxs.insert(pipIdxOfPreviousPIP);
        }
    }
}

template<typename U>
page_idx_t BaseDiskArray<U>::getAPPageIdxNoLock(page_idx_t apIdx, TransactionType trxType) {
    auto pipIdxAndOffset = StorageUtils::getQuotientRemainder(apIdx, NUM_PAGE_IDXS_PER_PIP);
    uint64_t pipIdx = pipIdxAndOffset.first;
    if ((trxType == TransactionType::READ_ONLY) || !hasPIPUpdatesNoLock(pipIdx)) {
        return pips[pipIdxAndOffset.first].pipContents.pageIdxs[pipIdxAndOffset.second];
    } else {
        page_idx_t retVal;
        page_idx_t pageIdxOfUpdatedPip = getUpdatedPageIdxOfPipNoLock(pipIdx);
        StorageStructureUtils::readWALVersionOfPage((VersionedFileHandle&)(fileHandle),
            pageIdxOfUpdatedPip, *bufferManager, *wal,
            [&retVal, &pipIdxAndOffset](uint8_t* frame) -> void {
                retVal = ((PIP*)frame)->pageIdxs[pipIdxAndOffset.second];
            });
        return retVal;
    }
}

template<typename U>
page_idx_t BaseDiskArray<U>::getUpdatedPageIdxOfPipNoLock(uint64_t pipIdx) {
    if (pipIdx < pips.size()) {
        return pips[pipIdx].pipPageIdx;
    }
    return pipUpdates.pipPageIdxsOfinsertedPIPs[pipIdx - pips.size()];
}

template<typename U>
void BaseDiskArray<U>::clearUpdatedWALPageVersionAndRemovePageFromFrameIfNecessary(
    page_idx_t pageIdx) {
    ((VersionedFileHandle&)this->fileHandle).clearUpdatedWALPageVersionIfNecessary(pageIdx);
    bufferManager->removePageFromFrameIfNecessary(this->fileHandle, pageIdx);
}

template<typename U>
void BaseDiskArray<U>::checkpointOrRollbackInMemoryIfNecessaryNoLock(bool isCheckpoint) {
    cout << "BaseDiskArray::checkpointOrRollbackInMemoryIfNecessaryNoLock called" << endl;
    if (!hasTransactionalUpdates) {
        return;
    }
    // Note: We update the header regardless (even if it has not changed). We can optimize this
    // by adding logic that keep track of whether the header has been updated.
    if (isCheckpoint) {
        cout << "Reading the header file during checkpointing. headerPageIdx: " << headerPageIdx
             << endl;
        header.readFromFile(this->fileHandle, headerPageIdx);
    }
    clearUpdatedWALPageVersionAndRemovePageFromFrameIfNecessary(headerPageIdx);
    for (uint64_t pipIdxOfUpdatedPIP : pipUpdates.updatedPipIdxs) {
        // Note: This should not cause a memory leak because PIPWrapper is a struct. So we
        // should overwrite the previous PIPWrapper's memory.
        if (isCheckpoint) {
            cout << "Refreshing an updated pip during checkpointing. pageIdx: "
                 << pips[pipIdxOfUpdatedPIP].pipPageIdx << endl;
            pips[pipIdxOfUpdatedPIP] = PIPWrapper(fileHandle, pips[pipIdxOfUpdatedPIP].pipPageIdx);
        }
        clearUpdatedWALPageVersionAndRemovePageFromFrameIfNecessary(
            pips[pipIdxOfUpdatedPIP].pipPageIdx);
    }

    for (page_idx_t pipPageIdxOfNewPIP : pipUpdates.pipPageIdxsOfinsertedPIPs) {
        if (isCheckpoint) {
            cout << "Refreshing a new pip page during checkpointing. pipPageIdxOfNewPIP: "
                 << pipPageIdxOfNewPIP << endl;
            pips.emplace_back(fileHandle, pipPageIdxOfNewPIP);
        }
        clearUpdatedWALPageVersionAndRemovePageFromFrameIfNecessary(pipPageIdxOfNewPIP);
        if (!isCheckpoint) {
            // These are newly inserted pages, so we can truncate the file handle.
            ((VersionedFileHandle&)this->fileHandle)
                .removePageIdxAndTruncateIfNecessary(pipPageIdxOfNewPIP);
        }
    }
    // Note that we already updated the header to its correct state above.
    clearPIPUpdatesAndIsArrayPageUpdated(header.numAPs);
    hasTransactionalUpdates = false;
    cout << "Exiting BaseDiskArray::checkpointOrRollbackInMemoryIfNecessaryNoLock called" << endl;
}

template<typename U>
bool BaseDiskArray<U>::hasPIPUpdatesNoLock(uint64_t pipIdx) {
    // Any request to a pipIdx > pips.size() which is the original number of pips we started
    // with before the write transaction is updated, so we return true.
    if (pipIdx >= pips.size()) {
        return true;
    }
    return pipUpdates.updatedPipIdxs.contains(pipIdx);
}

template<typename U>
uint64_t BaseDiskArray<U>::readUint64HeaderFieldNoLock(
    TransactionType trxType, std::function<uint64_t(DiskArrayHeader*)> readOp) {
    VersionedFileHandle* versionedFileHandle = (VersionedFileHandle*)(&fileHandle);
    if ((trxType == TransactionType::READ_ONLY) ||
        !versionedFileHandle->hasUpdatedWALPageVersionNoPageLock(headerPageIdx)) {
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
    } else if (apIdx == updatedDiskArrayHeader->numAPs) {
        // We need to add a new AP. This may further cause a new pip to be inserted, which is
        // handled by the if/else-if/else branch below.
        VersionedFileHandle& versionedFileHandle = (VersionedFileHandle&)this->fileHandle;
        page_idx_t newAPPageIdx = versionedFileHandle.addNewPage();
        // We need to create a new array page and then add its apPageIdx (newAPPageIdx variable) to
        // an appropriate PIP.
        auto pipIdxAndOffsetOfNewAP =
            StorageUtils::getQuotientRemainder(apIdx, NUM_PAGE_IDXS_PER_PIP);
        updatedDiskArrayHeader->numAPs++;
        uint64_t pipIdx = pipIdxAndOffsetOfNewAP.first;
        page_idx_t pipPageIdx = PAGE_IDX_MAX;
        bool isInsertingANewPIPPage = false;
        if (pipIdx < pips.size()) {
            // We do not need to insert a new pip and we need to add newAPPageIdx to a pip that
            // existed before this transaction started.
            pipUpdates.updatedPipIdxs.insert(pipIdx);
            pipPageIdx = pips[pipIdx].pipPageIdx;
        } else if ((pipIdx - pips.size()) < pipUpdates.pipPageIdxsOfinsertedPIPs.size()) {
            // We do not need to insert a new pip and we need to add newAPPageIdx to a new pip that
            // already got created after this transaction started.
            pipPageIdx = pipUpdates.pipPageIdxsOfinsertedPIPs[pipIdx - pips.size()];
        } else {
            // We need to create a new PIP and make the previous PIP (or the header) point to it.
            isInsertingANewPIPPage = true;
            pipPageIdx = versionedFileHandle.addNewPage();
            pipUpdates.pipPageIdxsOfinsertedPIPs.push_back(pipPageIdx);
            uint64_t pipIdxOfPreviousPIP = pipIdx - 1;
            setNextPIPPageIDxOfPIPNoLock(updatedDiskArrayHeader, pipIdxOfPreviousPIP, pipPageIdx);
        }
        // Finally we update the PIP page (possibly newly created) and add newAPPageIdx into it.
        StorageStructureUtils::updatePageTransactionally((VersionedFileHandle&)(fileHandle),
            pipPageIdx, isInsertingANewPIPPage, *bufferManager, *wal,
            [&isInsertingANewPIPPage, &newAPPageIdx, &pipIdxAndOffsetOfNewAP](
                uint8_t* frame) -> void {
                if (isInsertingANewPIPPage) {
                    ((PIP*)frame)->nextPipPageIdx = PAGE_IDX_MAX;
                }
                ((PIP*)frame)->pageIdxs[pipIdxAndOffsetOfNewAP.second] = newAPPageIdx;
            });
        return make_pair(newAPPageIdx, true /* inserting a new ap page */);
    } else {
        throw RuntimeException(
            "This else branch should never execute. apIdx: " + to_string(apIdx) +
            " updatedDiskArrayHeader->numArrayPages: " + to_string(updatedDiskArrayHeader->numAPs) +
            ". Inside BaseDiskArray<U>::getAPPageIdxAndAddAPToPIPIfNecessaryForWriteTrxNoLock");
    }
}

template<typename U>
void BaseDiskArray<U>::clearPIPUpdatesAndIsArrayPageUpdated(uint64_t numAPs) {
    pipUpdates.clearNoLock();
    fill(isArrayPageUpdated.begin(), isArrayPageUpdated.end(), false /* not updated*/);
    isArrayPageUpdated.resize(numAPs, false /* initialize all new pages to not updated */);
}

template<typename U>
void BaseDiskArray<U>::print() {
    cout << "Printing BaseDisk Array" << endl;
    cout << "headerPageIdx: " << headerPageIdx << endl;
    header.print();
    for (int i = 0; i < pips.size(); ++i) {
        pips[i].print();
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
                                   " of the element in DiskArray is < this->header.numElements: " +
                                   to_string(this->header.numElements) + " for read trx.");
        }
        return inMemArrayPages[apIdx][offsetInAP];
    }
    case WRITE: {
        if (idx >= this->getNumElementsNoLock(TransactionType::WRITE)) {
            throw RuntimeException("idx: " + to_string(idx) +
                                   " of the element in DiskArray is < this->header.numElements: " +
                                   to_string(this->header.numElements) + " for write trx.");
        }
        VersionedFileHandle& versionedFileHandle = (VersionedFileHandle&)this->fileHandle;
        // First check if the corresponding pip has a WALVersion or not, that will let us
        // find the physicalPageIdx
        page_idx_t pageIdx = this->getAPPageIdxNoLock(apIdx, TransactionType::WRITE);
        if (versionedFileHandle.hasUpdatedWALPageVersionNoPageLock(pageIdx)) {
            U retVal;
            StorageStructureUtils::readWALVersionOfPage(versionedFileHandle, pageIdx,
                *this->bufferManager, *this->wal, [&retVal, &offsetInAP](uint8_t* frame) -> void {
                    retVal = ((U*)frame)[offsetInAP];
                });
            return retVal;
        } else {
            // There is no updates, so we can directly read from the inMemArrayPages in memory.
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

template<typename U>
void BaseInMemDiskArray<U>::print() {
    BaseDiskArray<U>::print();
    for (page_idx_t apIdx = 0; apIdx < this->header.numAPs; ++apIdx) {
        auto numElementsPerPage = 1 << this->header.numElementsPerPageLog2;
        cout << "array page " << apIdx << " (file pageIdx: " << this->getAPPageIdxNoLock(apIdx)
             << ") contents: ";
        for (int j = 0; j < numElementsPerPage; ++j) {
            cout << " " << inMemArrayPages[apIdx][j];
        }
        cout << endl;
        inMemArrayPages.emplace_back(make_unique<U[]>(1 << this->header.numElementsPerPageLog2));
    }
}

template<typename T>
InMemDiskArray<T>::InMemDiskArray(VersionedFileHandle& fileHandle, page_idx_t headerPageIdx,
    BufferManager* bufferManager, WAL* wal)
    : BaseInMemDiskArray<T>(fileHandle, headerPageIdx, bufferManager, wal) {}

template<typename T>
void InMemDiskArray<T>::checkpointInMemoryIfNecessary() {
    //    cout << "InMemDiskArray::checkpointInMemoryIfNecessary called" << endl;
    unique_lock xlock{this->diskArraySharedMtx};
    checkpointOrRollbackInMemoryIfNecessaryNoLock(true /* is checkpoint */);
    //    cout << "Exiting InMemDiskArray::checkpointInMemoryIfNecessary" << endl;
}

template<typename T>
void InMemDiskArray<T>::rollbackInMemoryIfNecessary() {
    //    cout << "InMemDiskArray::rollbackInMemoryIfNecessary called" << endl;
    unique_lock xlock{this->diskArraySharedMtx};
    InMemDiskArray<T>::checkpointOrRollbackInMemoryIfNecessaryNoLock(false /* is rollback */);
    //    cout << "Exiting InMemDiskArray::rollbackInMemoryIfNecessary" << endl;
}

template<typename T>
void InMemDiskArray<T>::checkpointOrRollbackInMemoryIfNecessaryNoLock(bool isCheckpoint) {
    if (!this->hasTransactionalUpdates) {
        return;
    }
    for (uint64_t apIdx = 0; apIdx < this->isArrayPageUpdated.size(); ++apIdx) {
        if (this->isArrayPageUpdated[apIdx]) {
            // We are directly reading the new image from disk.
            page_idx_t apPageIdx = this->getAPPageIdxNoLock(apIdx, TransactionType::WRITE);
            if (isCheckpoint) {
                this->readArrayPageFromFile(apIdx, apPageIdx);
            }
            this->clearUpdatedWALPageVersionAndRemovePageFromFrameIfNecessary(apPageIdx);
        }
    }
    uint64_t newNumAPs = this->getNumAPsNoLock(TransactionType::WRITE);
    // When rolling back, unlike removing new PIPs in
    // BaseDiskArray::checkpointOrRollbackInMemoryIfNecessaryNoLock when rolling back, we cannot
    // directly truncate each page. Instead we need to keep track of the minimum apPageIdx we
    // saw that we want to truncate to first. Then we call
    // BaseDiskArray::checkpointOrRollbackInMemoryIfNecessaryNoLock, which can do its own truncation
    // due to newly added PIPs. Then finally we truncate. The reason is this: suppose we added
    // a new apIdx=1 with pageIdx 20, which suppose caused a new PIP to be inserted with pageIdx 21,
    // and we further added one more new apIdx=2 with pageIdx 22. Now this function will loop
    // through the newly added apIdxs, so apIdx=1 and 2 in that order. If we directly truncate
    // to the pageIdx of apIdx=1, which is 20, then we will remove 21 and 22. But then we will
    // loop through apIdx=2 and we need to convert it to its pageIdx to clear its updated WAL
    // version. But that requires the newly added PIP, which had pageIdx 22 but no longer exists and
    // get a seg fault somewhere. So we do not truncate these in order not to accidentally remove
    // newly added PIPs.
    page_idx_t minNewAPPageIdxToTruncateTo = PAGE_IDX_MAX;
    for (uint64_t apIdx = this->header.numAPs; apIdx < newNumAPs; apIdx++) {
        page_idx_t apPageIdx = this->getAPPageIdxNoLock(apIdx, TransactionType::WRITE);
        if (isCheckpoint) {
            this->addInMemoryArrayPageAndReadFromFile(apPageIdx);
        }
        this->clearUpdatedWALPageVersionAndRemovePageFromFrameIfNecessary(apPageIdx);
        if (!isCheckpoint) {
            page_idx_t minNewAPPageIdxToTruncateTo = min(minNewAPPageIdxToTruncateTo, apPageIdx);
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
    cout << "saving headers and pips. " << endl;
    this->header.print();
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
    // find the pipIdx and offset in the pip of the array page before incrementing
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
