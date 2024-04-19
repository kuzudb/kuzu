#include "storage/storage_structure/disk_array.h"

#include "common/cast.h"
#include "common/constants.h"
#include "common/string_format.h"
#include "common/types/types.h"
#include "common/utils.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/file_handle.h"
#include "storage/storage_structure/db_file_utils.h"
#include "storage/storage_utils.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

DiskArrayHeader::DiskArrayHeader(uint64_t elementSize)
    : alignedElementSizeLog2{(uint64_t)ceil(log2(elementSize))},
      numElementsPerPageLog2{BufferPoolConstants::PAGE_4KB_SIZE_LOG2 - alignedElementSizeLog2},
      elementPageOffsetMask{BitmaskUtils::all1sMaskForLeastSignificantBits(numElementsPerPageLog2)},
      firstPIPPageIdx{DBFileUtils::NULL_PAGE_IDX}, numElements{0}, numAPs{0} {
    KU_ASSERT(elementSize <= BufferPoolConstants::PAGE_4KB_SIZE);
}

void DiskArrayHeader::saveToDisk(FileHandle& fileHandle, uint64_t headerPageIdx) {
    fileHandle.getFileInfo()->writeFile(reinterpret_cast<uint8_t*>(this), sizeof(DiskArrayHeader),
        headerPageIdx * fileHandle.getPageSize());
}

void DiskArrayHeader::readFromFile(FileHandle& fileHandle, uint64_t headerPageIdx) {
    fileHandle.getFileInfo()->readFromFile(reinterpret_cast<uint8_t*>(this),
        sizeof(DiskArrayHeader), headerPageIdx * fileHandle.getPageSize());
}

PIPWrapper::PIPWrapper(FileHandle& fileHandle, page_idx_t pipPageIdx) : pipPageIdx(pipPageIdx) {
    fileHandle.readPage(reinterpret_cast<uint8_t*>(&pipContents), pipPageIdx);
}

BaseDiskArrayInternal::BaseDiskArrayInternal(FileHandle& fileHandle, page_idx_t headerPageIdx,
    uint64_t elementSize)
    : header{elementSize}, fileHandle{fileHandle}, headerPageIdx{headerPageIdx},
      headerForWriteTrx{header}, hasTransactionalUpdates{false}, bufferManager{nullptr},
      wal{nullptr}, lastAPPageIdx{INVALID_PAGE_IDX} {}

BaseDiskArrayInternal::BaseDiskArrayInternal(FileHandle& fileHandle, DBFileID dbFileID,
    page_idx_t headerPageIdx, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, bool bypassWAL)
    : fileHandle{fileHandle}, dbFileID{dbFileID}, headerPageIdx{headerPageIdx},
      hasTransactionalUpdates{false}, bufferManager{bufferManager}, wal{wal},
      lastAPPageIdx{INVALID_PAGE_IDX}, lastPageOnDisk{INVALID_PAGE_IDX} {
    auto [fileHandleToPin, pageIdxToPin] = DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
        ku_dynamic_cast<FileHandle&, BMFileHandle&>(fileHandle), headerPageIdx, *wal,
        transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin,
        [&](uint8_t* frame) -> void { memcpy(&header, frame, sizeof(DiskArrayHeader)); });
    headerForWriteTrx = header;
    if (this->header.firstPIPPageIdx != DBFileUtils::NULL_PAGE_IDX) {
        pips.emplace_back(fileHandle, header.firstPIPPageIdx);
        while (pips[pips.size() - 1].pipContents.nextPipPageIdx != DBFileUtils::NULL_PAGE_IDX) {
            pips.emplace_back(fileHandle, pips[pips.size() - 1].pipContents.nextPipPageIdx);
        }
    }
    // If bypassing the WAL is disabled, just leave the lastPageOnDisk as invalid, as then all pages
    // will be treated as updates to existing ones
    if (bypassWAL) {
        updateLastPageOnDisk();
    }
}

void BaseDiskArrayInternal::updateLastPageOnDisk() {
    auto numElements = getNumElementsNoLock(TransactionType::READ_ONLY);
    if (numElements > 0) {
        auto apCursor = getAPIdxAndOffsetInAP(numElements - 1);
        lastPageOnDisk = getAPPageIdxNoLock(apCursor.pageIdx, TransactionType::READ_ONLY);
    } else {
        lastPageOnDisk = 0;
    }
}

uint64_t BaseDiskArrayInternal::getNumElements(TransactionType trxType) {
    std::shared_lock sLck{diskArraySharedMtx};
    return getNumElementsNoLock(trxType);
}

bool BaseDiskArrayInternal::checkOutOfBoundAccess(TransactionType trxType, uint64_t idx) {
    auto currentNumElements = getNumElementsNoLock(trxType);
    if (idx >= currentNumElements) {
        // LCOV_EXCL_START
        throw RuntimeException(stringFormat(
            "idx: {} of the DiskArray to be accessed is >= numElements in DiskArray{}.", idx,
            currentNumElements));
        // LCOV_EXCL_STOP
    }
    return true;
}

void BaseDiskArrayInternal::get(uint64_t idx, TransactionType trxType, std::span<uint8_t> val) {
    std::shared_lock sLck{diskArraySharedMtx};
    KU_ASSERT(checkOutOfBoundAccess(trxType, idx));
    auto apCursor = getAPIdxAndOffsetInAP(idx);
    page_idx_t apPageIdx = getAPPageIdxNoLock(apCursor.pageIdx, trxType);
    auto& bmFileHandle = (BMFileHandle&)fileHandle;
    if (trxType == TransactionType::READ_ONLY || !hasTransactionalUpdates ||
        apPageIdx > lastPageOnDisk || !bmFileHandle.hasWALPageVersionNoWALPageIdxLock(apPageIdx)) {
        bufferManager->optimisticRead(bmFileHandle, apPageIdx, [&](const uint8_t* frame) -> void {
            memcpy(val.data(), frame + apCursor.elemPosInPage, val.size());
        });
    } else {
        ((BMFileHandle&)fileHandle).acquireWALPageIdxLock(apPageIdx);
        DBFileUtils::readWALVersionOfPage(bmFileHandle, apPageIdx, *bufferManager, *wal,
            [&val, &apCursor](const uint8_t* frame) -> void {
                memcpy(val.data(), frame + apCursor.elemPosInPage, val.size());
            });
    }
}

void BaseDiskArrayInternal::updatePage(uint64_t pageIdx, bool isNewPage,
    std::function<void(uint8_t*)> updateOp) {
    auto& bmFileHandle = (BMFileHandle&)fileHandle;
    // Pages which are new to this transaction are written directly to the file
    // Pages which previously existed are written to the WAL file
    if (pageIdx <= lastPageOnDisk) {
        // This may still be used to create new pages since bypassing the WAL is currently optional
        // and if disabled lastPageOnDisk will be INVALID_PAGE_IDX (and the above comparison will
        // always be true)
        DBFileUtils::updatePage(bmFileHandle, dbFileID, pageIdx, isNewPage, *bufferManager, *wal,
            updateOp);
    } else {
        auto frame = bufferManager->pin(bmFileHandle, pageIdx,
            isNewPage ? BufferManager::PageReadPolicy::DONT_READ_PAGE :
                        BufferManager::PageReadPolicy::READ_PAGE);
        updateOp(frame);
        bmFileHandle.setLockedPageDirty(pageIdx);
        bufferManager->unpin(bmFileHandle, pageIdx);
    }
}

void BaseDiskArrayInternal::update(uint64_t idx, std::span<uint8_t> val) {
    std::unique_lock xLck{diskArraySharedMtx};
    hasTransactionalUpdates = true;
    KU_ASSERT(checkOutOfBoundAccess(TransactionType::WRITE, idx));
    auto apCursor = getAPIdxAndOffsetInAP(idx);
    // TODO: We are currently supporting only DiskArrays that can grow in size and not
    // those that can shrink in size. That is why we can use
    // getAPPageIdxNoLock(apIdx, Transaction::WRITE) directly to compute the physical page Idx
    // because any apIdx is guaranteed to be either in an existing PIP or a new PIP we added, which
    // getAPPageIdxNoLock will correctly locate: this function simply searches an existing PIP if
    // apIdx < numAPs stored in "previous" PIP; otherwise one of the newly inserted PIPs stored in
    // pipPageIdxsOfInsertedPIPs. If within a single transaction we could grow or shrink, then
    // getAPPageIdxNoLock logic needs to change to give the same guarantee (e.g., an apIdx = 0, may
    // no longer to be guaranteed to be in pips[0].)
    page_idx_t apPageIdx = getAPPageIdxNoLock(apCursor.pageIdx, TransactionType::WRITE);
    updatePage(apPageIdx, false /*isNewPage=*/, [&apCursor, &val](uint8_t* frame) -> void {
        memcpy(frame + apCursor.elemPosInPage, val.data(), val.size());
    });
}

uint64_t BaseDiskArrayInternal::pushBack(std::span<uint8_t> val) {
    std::unique_lock xLck{diskArraySharedMtx};
    auto it = iter_mut(val.size());
    auto originalNumElements = getNumElementsNoLock(TransactionType::WRITE);
    it.pushBack(val);
    return originalNumElements;
}

uint64_t BaseDiskArrayInternal::resize(uint64_t newNumElements, std::span<uint8_t> defaultVal) {
    std::unique_lock xLck{diskArraySharedMtx};
    auto it = iter_mut(defaultVal.size());
    auto originalNumElements = getNumElementsNoLock(TransactionType::WRITE);
    while (it.size() < newNumElements) {
        it.pushBack(defaultVal);
    }
    return originalNumElements;
}

void BaseDiskArrayInternal::setNextPIPPageIDxOfPIPNoLock(DiskArrayHeader* updatedDiskArrayHeader,
    uint64_t pipIdxOfPreviousPIP, page_idx_t nextPIPPageIdx) {
    // This happens if the first pip is being inserted, in which case we need to change the header.
    if (pipIdxOfPreviousPIP == UINT64_MAX) {
        updatedDiskArrayHeader->firstPIPPageIdx = nextPIPPageIdx;
    } else if (pips.empty()) {
        pipUpdates.newPIPs[pipIdxOfPreviousPIP].pipContents.nextPipPageIdx = nextPIPPageIdx;
    } else {
        if (!pipUpdates.updatedLastPIP.has_value()) {
            pipUpdates.updatedLastPIP = std::make_optional(pips[pipIdxOfPreviousPIP]);
        }
        if (pipIdxOfPreviousPIP == pips.size() - 1) {
            pipUpdates.updatedLastPIP->pipContents.nextPipPageIdx = nextPIPPageIdx;
        } else {
            KU_ASSERT(pipIdxOfPreviousPIP >= pips.size() &&
                      pipUpdates.newPIPs.size() > pipIdxOfPreviousPIP - pips.size());
            pipUpdates.newPIPs[pipIdxOfPreviousPIP - pips.size()].pipContents.nextPipPageIdx =
                nextPIPPageIdx;
        }
    }
}

page_idx_t BaseDiskArrayInternal::getAPPageIdxNoLock(page_idx_t apIdx, TransactionType trxType) {
    auto [pipIdx, offsetInPIP] = StorageUtils::getQuotientRemainder(apIdx, NUM_PAGE_IDXS_PER_PIP);
    if ((trxType == TransactionType::READ_ONLY) || !hasPIPUpdatesNoLock(pipIdx)) {
        return pips[pipIdx].pipContents.pageIdxs[offsetInPIP];
    } else if (pipIdx == pips.size() - 1 && pipUpdates.updatedLastPIP) {
        return pipUpdates.updatedLastPIP->pipContents.pageIdxs[offsetInPIP];
    } else {
        KU_ASSERT(pipIdx >= pips.size() && pipIdx - pips.size() < pipUpdates.newPIPs.size());
        return pipUpdates.newPIPs[pipIdx - pips.size()].pipContents.pageIdxs[offsetInPIP];
    }
}

page_idx_t BaseDiskArrayInternal::getUpdatedPageIdxOfPipNoLock(uint64_t pipIdx) {
    if (pipIdx < pips.size()) {
        return pips[pipIdx].pipPageIdx;
    }
    return pipUpdates.newPIPs[pipIdx - pips.size()].pipPageIdx;
}

void BaseDiskArrayInternal::clearWALPageVersionAndRemovePageFromFrameIfNecessary(
    page_idx_t pageIdx) {
    ((BMFileHandle&)this->fileHandle).clearWALPageIdxIfNecessary(pageIdx);
    bufferManager->removePageFromFrameIfNecessary((BMFileHandle&)this->fileHandle, pageIdx);
}

void BaseDiskArrayInternal::checkpointOrRollbackInMemoryIfNecessaryNoLock(bool isCheckpoint) {
    if (!hasTransactionalUpdates) {
        return;
    }
    if (isCheckpoint) {
        header = headerForWriteTrx;
    } else {
        headerForWriteTrx = header;
    }
    clearWALPageVersionAndRemovePageFromFrameIfNecessary(headerPageIdx);
    if (pipUpdates.updatedLastPIP.has_value()) {
        // Note: This should not cause a memory leak because PIPWrapper is a struct. So we
        // should overwrite the previous PIPWrapper's memory.
        if (isCheckpoint) {
            pips.back() = *pipUpdates.updatedLastPIP;
        }
        clearWALPageVersionAndRemovePageFromFrameIfNecessary(pips.back().pipPageIdx);
    }

    for (auto& newPIP : pipUpdates.newPIPs) {
        clearWALPageVersionAndRemovePageFromFrameIfNecessary(newPIP.pipPageIdx);
        if (isCheckpoint) {
            pips.emplace_back(std::move(newPIP));
        } else {
            // These are newly inserted pages, so we can truncate the file handle.
            ((BMFileHandle&)this->fileHandle)
                .removePageIdxAndTruncateIfNecessary(newPIP.pipPageIdx);
        }
    }
    // Note that we already updated the header to its correct state above.
    pipUpdates.clear();
    hasTransactionalUpdates = false;
    if (isCheckpoint && lastPageOnDisk != INVALID_PAGE_IDX) {
        updateLastPageOnDisk();
    }
}

void BaseDiskArrayInternal::prepareCommit() {
    auto& bmFileHandle = ku_dynamic_cast<FileHandle&, BMFileHandle&>(fileHandle);
    // Update header if it has changed
    if (headerForWriteTrx != header) {
        DBFileUtils::updatePage(bmFileHandle, dbFileID, headerPageIdx,
            false /* not inserting a new page */, *bufferManager, *wal,
            [&](uint8_t* frame) -> void {
                memcpy(frame, &headerForWriteTrx, sizeof(headerForWriteTrx));
            });
    }
    if (pipUpdates.updatedLastPIP.has_value()) {
        DBFileUtils::updatePage(bmFileHandle, dbFileID, pipUpdates.updatedLastPIP->pipPageIdx, true,
            *bufferManager, *wal, [&](auto* frame) {
                memcpy(frame, &pipUpdates.updatedLastPIP->pipContents, sizeof(PIP));
            });
    }
    for (auto& newPIP : pipUpdates.newPIPs) {
        DBFileUtils::updatePage(bmFileHandle, dbFileID, newPIP.pipPageIdx, true, *bufferManager,
            *wal, [&](auto* frame) { memcpy(frame, &newPIP.pipContents, sizeof(PIP)); });
    }
}

bool BaseDiskArrayInternal::hasPIPUpdatesNoLock(uint64_t pipIdx) {
    // This is a request to a pipIdx > pips.size(). Since pips.size() is the original number of pips
    // we started with before the write transaction is updated, we return true, i.e., this PIP is
    // an "updated" PIP and should be read from the WAL version.
    if (pipIdx >= pips.size()) {
        return true;
    }
    return (pipIdx == pips.size() - 1) && pipUpdates.updatedLastPIP;
}

std::pair<page_idx_t, bool>
BaseDiskArrayInternal::getAPPageIdxAndAddAPToPIPIfNecessaryForWriteTrxNoLock(
    DiskArrayHeader* updatedDiskArrayHeader, page_idx_t apIdx) {
    if (apIdx == updatedDiskArrayHeader->numAPs - 1 && lastAPPageIdx != INVALID_PAGE_IDX) {
        return std::make_pair(lastAPPageIdx, false /*not a new page*/);
    } else if (apIdx < updatedDiskArrayHeader->numAPs) {
        // If the apIdx of the array page is < updatedDiskArrayHeader->numAPs, we do not have to
        // add a new array page, so directly return the pageIdx of the apIdx.
        return std::make_pair(getAPPageIdxNoLock(apIdx, TransactionType::WRITE),
            false /* is not inserting a new ap page */);
    } else {
        // apIdx even if it's being inserted should never be > updatedDiskArrayHeader->numAPs.
        KU_ASSERT(apIdx == updatedDiskArrayHeader->numAPs);
        // We need to add a new AP. This may further cause a new pip to be inserted, which is
        // handled by the if/else-if/else branch below.
        page_idx_t newAPPageIdx = fileHandle.addNewPage();
        // We need to create a new array page and then add its apPageIdx (newAPPageIdx variable) to
        // an appropriate PIP.
        auto pipIdxAndOffsetOfNewAP =
            StorageUtils::getQuotientRemainder(apIdx, NUM_PAGE_IDXS_PER_PIP);
        uint64_t pipIdx = pipIdxAndOffsetOfNewAP.first;
        uint64_t offsetOfNewAPInPIP = pipIdxAndOffsetOfNewAP.second;
        updatedDiskArrayHeader->numAPs++;
        if (pipIdx < pips.size()) {
            KU_ASSERT(pipIdx == pips.size() - 1);
            // We do not need to insert a new pip and we need to add newAPPageIdx to a PIP that
            // existed before this transaction started.
            if (!pipUpdates.updatedLastPIP.has_value()) {
                pipUpdates.updatedLastPIP = std::make_optional(pips[pipIdx]);
            }
            pipUpdates.updatedLastPIP->pipContents.pageIdxs[offsetOfNewAPInPIP] = newAPPageIdx;
        } else if ((pipIdx - pips.size()) < pipUpdates.newPIPs.size()) {
            // We do not need to insert a new PIP and we need to add newAPPageIdx to a new PIP that
            // already got created after this transaction started.
            auto& pip = pipUpdates.newPIPs[pipIdx - pips.size()];
            pip.pipContents.pageIdxs[offsetOfNewAPInPIP] = newAPPageIdx;
        } else {
            // We need to create a new PIP and make the previous PIP (or the header) point to it.
            auto pipPageIdx = fileHandle.addNewPage();
            pipUpdates.newPIPs.emplace_back(pipPageIdx);
            uint64_t pipIdxOfPreviousPIP = pipIdx - 1;
            setNextPIPPageIDxOfPIPNoLock(updatedDiskArrayHeader, pipIdxOfPreviousPIP, pipPageIdx);
            pipUpdates.newPIPs.back().pipContents.pageIdxs[offsetOfNewAPInPIP] = newAPPageIdx;
        }
        return std::make_pair(newAPPageIdx, true /* inserting a new ap page */);
    }
}

BaseDiskArrayInternal::WriteIterator& BaseDiskArrayInternal::WriteIterator::seek(size_t newIdx) {
    KU_ASSERT(newIdx < diskArray.headerForWriteTrx.numElements);
    auto oldPageIdx = apCursor.pageIdx;
    idx = newIdx;
    apCursor = diskArray.getAPIdxAndOffsetInAP(idx);
    if (oldPageIdx != apCursor.pageIdx) {
        common::page_idx_t apPageIdx = diskArray.getAPPageIdxNoLock(apCursor.pageIdx, TRX_TYPE);
        getPage(apPageIdx, false /*isNewlyAdded*/);
    }
    return *this;
}

void BaseDiskArrayInternal::WriteIterator::pushBack(std::span<uint8_t> val) {
    idx = diskArray.headerForWriteTrx.numElements++;
    auto oldPageIdx = apCursor.pageIdx;
    apCursor = diskArray.getAPIdxAndOffsetInAP(idx);
    // If this would add a new page, pin new page and update PIP
    auto [apPageIdx, isNewlyAdded] =
        diskArray.getAPPageIdxAndAddAPToPIPIfNecessaryForWriteTrxNoLock(
            &diskArray.headerForWriteTrx, apCursor.pageIdx);
    diskArray.lastAPPageIdx = apPageIdx;
    if (isNewlyAdded || walPageIdxAndFrame.originalPageIdx == common::INVALID_PAGE_IDX ||
        apCursor.pageIdx != oldPageIdx) {
        getPage(apPageIdx, isNewlyAdded);
    }
    memcpy(operator*().data(), val.data(), val.size());
}

void BaseDiskArrayInternal::WriteIterator::unpin() {
    auto& bmFileHandle = common::ku_dynamic_cast<FileHandle&, BMFileHandle&>(diskArray.fileHandle);
    if (walPageIdxAndFrame.pageIdxInWAL != common::INVALID_PAGE_IDX) {
        // unpin current page
        diskArray.bufferManager->unpin(diskArray.wal->getShadowingFH(),
            walPageIdxAndFrame.pageIdxInWAL);
        bmFileHandle.releaseWALPageIdxLock(walPageIdxAndFrame.originalPageIdx);
    } else if (walPageIdxAndFrame.originalPageIdx != common::INVALID_PAGE_IDX) {
        bmFileHandle.setLockedPageDirty(walPageIdxAndFrame.originalPageIdx);
        diskArray.bufferManager->unpin(bmFileHandle, walPageIdxAndFrame.originalPageIdx);
    }
}

void BaseDiskArrayInternal::WriteIterator::getPage(common::page_idx_t newPageIdx,
    bool isNewlyAdded) {
    auto& bmFileHandle = common::ku_dynamic_cast<FileHandle&, BMFileHandle&>(diskArray.fileHandle);
    unpin();
    if (newPageIdx <= diskArray.lastPageOnDisk) {
        // Pin new page
        walPageIdxAndFrame =
            DBFileUtils::createWALVersionIfNecessaryAndPinPage(newPageIdx, isNewlyAdded,
                bmFileHandle, diskArray.dbFileID, *diskArray.bufferManager, *diskArray.wal);
    } else {
        walPageIdxAndFrame.frame = diskArray.bufferManager->pin(bmFileHandle, newPageIdx,
            isNewlyAdded ? BufferManager::PageReadPolicy::DONT_READ_PAGE :
                           BufferManager::PageReadPolicy::READ_PAGE);
        walPageIdxAndFrame.originalPageIdx = newPageIdx;
        walPageIdxAndFrame.pageIdxInWAL = common::INVALID_PAGE_IDX;
    }
}

BaseDiskArrayInternal::WriteIterator BaseDiskArrayInternal::iter_mut(uint64_t valueSize) {
    return BaseDiskArrayInternal::WriteIterator(valueSize, *this);
}

BaseInMemDiskArray::BaseInMemDiskArray(FileHandle& fileHandle, DBFileID dbFileID,
    page_idx_t headerPageIdx, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction)
    : BaseDiskArrayInternal(fileHandle, dbFileID, headerPageIdx, bufferManager, wal, transaction) {
    for (page_idx_t apIdx = 0; apIdx < this->header.numAPs; ++apIdx) {
        addInMemoryArrayPageAndReadFromFile(this->getAPPageIdxNoLock(apIdx));
    }
}

BaseInMemDiskArray::BaseInMemDiskArray(FileHandle& fileHandle, page_idx_t headerPageIdx,
    uint64_t elementSize)
    : BaseDiskArrayInternal(fileHandle, headerPageIdx, elementSize) {}

// [] operator to be used when building an InMemDiskArrayBuilder without transactional updates.
// This changes the contents directly in memory and not on disk (nor on the wal)
uint8_t* BaseInMemDiskArray::operator[](uint64_t idx) {
    auto apCursor = BaseDiskArrayInternal::getAPIdxAndOffsetInAP(idx);
    KU_ASSERT(apCursor.pageIdx < this->header.numAPs);
    return inMemArrayPages[apCursor.pageIdx].get() + apCursor.elemPosInPage;
}

void BaseInMemDiskArray::addInMemoryArrayPageAndReadFromFile(page_idx_t apPageIdx) {
    uint64_t apIdx = this->addInMemoryArrayPage(false /* setToZero */);
    readArrayPageFromFile(apIdx, apPageIdx);
}

void BaseInMemDiskArray::readArrayPageFromFile(uint64_t apIdx, page_idx_t apPageIdx) {
    this->fileHandle.readPage(reinterpret_cast<uint8_t*>(this->inMemArrayPages[apIdx].get()),
        apPageIdx);
}

InMemDiskArrayBuilderInternal::InMemDiskArrayBuilderInternal(FileHandle& fileHandle,
    page_idx_t headerPageIdx, uint64_t numElements, size_t elementSize, bool setToZero)
    : BaseInMemDiskArray(fileHandle, headerPageIdx, elementSize) {
    setNumElementsAndAllocateDiskAPsForBuilding(numElements);
    for (uint64_t i = 0; i < this->header.numAPs; ++i) {
        this->addInMemoryArrayPage(setToZero);
    }
}

void InMemDiskArrayBuilderInternal::resize(uint64_t newNumElements, bool setToZero) {
    uint64_t oldNumAPs = this->header.numAPs;
    setNumElementsAndAllocateDiskAPsForBuilding(newNumElements);
    uint64_t newNumAPs = this->header.numAPs;
    for (auto i = oldNumAPs; i < newNumAPs; ++i) {
        this->addInMemoryArrayPage(setToZero);
    }
}

void InMemDiskArrayBuilderInternal::saveToDisk() {
    // save the header and pips.
    this->header.saveToDisk(this->fileHandle, this->headerPageIdx);
    for (auto i = 0u; i < this->pips.size(); ++i) {
        this->fileHandle.writePage(reinterpret_cast<uint8_t*>(&this->pips[i].pipContents),
            this->pips[i].pipPageIdx);
    }
    // Save array pages
    for (page_idx_t apIdx = 0; apIdx < this->header.numAPs; ++apIdx) {
        this->fileHandle.writePage(reinterpret_cast<uint8_t*>(this->inMemArrayPages[apIdx].get()),
            this->getAPPageIdxNoLock(apIdx));
    }
}

void InMemDiskArrayBuilderInternal::addNewArrayPageForBuilding() {
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

void InMemDiskArrayBuilderInternal::setNumElementsAndAllocateDiskAPsForBuilding(
    uint64_t newNumElements) {
    uint64_t oldNumArrayPages = this->header.numAPs;
    uint64_t newNumArrayPages = getNumArrayPagesNeededForElements(newNumElements);
    for (auto i = oldNumArrayPages; i < newNumArrayPages; ++i) {
        addNewArrayPageForBuilding();
    }
    this->header.numElements = newNumElements;
    this->header.numAPs = newNumArrayPages;
}

} // namespace storage
} // namespace kuzu
