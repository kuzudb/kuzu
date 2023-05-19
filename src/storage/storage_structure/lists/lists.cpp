#include "storage/storage_structure/lists/lists.h"

#include "storage/storage_structure/lists/lists_update_iterator.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

// Note: The given nodeOffset and largeListHandle may not be connected. For example if we
// are about to read a new nodeOffset, say v5, after having read a previous nodeOffset, say v7, with
// a largeList, then the input to this function can be nodeOffset: 5 and largeListHandle containing
// information about the last portion of v7's large list. Similarly, if nodeOffset is v3 and v3
// has a small list then largeListHandle does not contain anything specific to v3 (it would likely
// be containing information about the last portion of the last large list that was read).
void Lists::readValues(Transaction* transaction, ValueVector* valueVector, ListHandle& listHandle) {
    if (listHandle.getListSourceStore() == ListSourceStore::UPDATE_STORE) {
        listsUpdatesStore->readValues(
            storageStructureIDAndFName.storageStructureID.listFileID, listHandle, valueVector);
    } else {
        // If the startElementOffset is 0, it means that this is the first time that we read from
        // the list. As a result, we need to reset the cursor and mapper.
        if (listHandle.getStartElemOffset() == 0) {
            listHandle.setMapper(metadata);
        }
        readFromList(valueVector, listHandle);
        if (transaction->isWriteTransaction()) {
            listsUpdatesStore->readUpdatesToPropertyVectorIfExists(
                storageStructureIDAndFName.storageStructureID.listFileID,
                listHandle.getBoundNodeOffset(), valueVector, listHandle.getStartElemOffset());
        }
    }
}

void Lists::readFromList(common::ValueVector* valueVector, ListHandle& listHandle) {
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        headers->getCSROffset(listHandle.getBoundNodeOffset()) + listHandle.getStartElemOffset(),
        numElementsPerPage);
    readBySequentialCopy(
        Transaction::getDummyReadOnlyTrx().get(), valueVector, pageCursor, listHandle.mapper);
}

uint64_t Lists::getNumElementsInPersistentStore(
    TransactionType transactionType, offset_t nodeOffset) {
    if (transactionType == TransactionType::WRITE &&
        listsUpdatesStore->isNewlyAddedNode(
            storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset)) {
        return 0;
    }
    return getNumElementsFromListHeader(nodeOffset);
}

void Lists::initListReadingState(
    offset_t nodeOffset, ListHandle& listHandle, TransactionType transactionType) {
    listHandle.resetSyncState();
    auto isNewlyAddedNode = listsUpdatesStore->isNewlyAddedNode(
        storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset);
    uint64_t numElementsInPersistentStore, numElementsInUpdateStore = 0;
    if (transactionType == TransactionType::WRITE) {
        numElementsInUpdateStore = listsUpdatesStore->getNumInsertedRelsForNodeOffset(
            storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset);
        // ListHeader is UINT32_MAX in two cases: (i) ListSyncState is not initialized; or (ii)
        // the list of a new node is being scanned so there is no header for the new node.
        numElementsInPersistentStore =
            isNewlyAddedNode ? 0 : getNumElementsFromListHeader(nodeOffset);
    } else {
        numElementsInPersistentStore = getNumElementsFromListHeader(nodeOffset);
    }
    // If there's no element is persistentStore, we can skip reading from persistentStore and start
    // reading from listsUpdatesStore directly.
    auto sourceStore = numElementsInPersistentStore == 0 ? ListSourceStore::UPDATE_STORE :
                                                           ListSourceStore::PERSISTENT_STORE;
    listHandle.initSyncState(
        nodeOffset, numElementsInUpdateStore, numElementsInPersistentStore, sourceStore);
}

std::unique_ptr<InMemList> Lists::createInMemListWithDataFromUpdateStoreOnly(
    offset_t nodeOffset, std::vector<uint64_t>& insertedRelsTupleIdxInFT) {
    auto inMemList = std::make_unique<InMemList>(
        getNumElementsInListsUpdatesStore(nodeOffset), elementSize, mayContainNulls());
    listsUpdatesStore->readInsertedRelsToList(
        storageStructureIDAndFName.storageStructureID.listFileID, insertedRelsTupleIdxInFT,
        *inMemList, 0 /* numElementsInPersistentStore */, getDiskOverflowFileIfExists(), dataType);
    return inMemList;
}

std::unique_ptr<InMemList> Lists::writeToInMemList(offset_t nodeOffset,
    const std::vector<uint64_t>& insertedRelTupleIdxesInFT,
    const std::unordered_set<uint64_t>& deletedRelOffsetsForList,
    UpdatedPersistentListOffsets* updatedPersistentListOffsets) {
    auto inMemList =
        std::make_unique<InMemList>(getTotalNumElementsInList(TransactionType::WRITE, nodeOffset),
            elementSize, mayContainNulls());
    auto numElementsInPersistentStore = getNumElementsFromListHeader(nodeOffset);
    fillInMemListsFromPersistentStore(nodeOffset, numElementsInPersistentStore, *inMemList,
        deletedRelOffsetsForList, updatedPersistentListOffsets);
    listsUpdatesStore->readInsertedRelsToList(
        storageStructureIDAndFName.storageStructureID.listFileID, insertedRelTupleIdxesInFT,
        *inMemList, numElementsInPersistentStore - deletedRelOffsetsForList.size(),
        getDiskOverflowFileIfExists(), dataType);
    return inMemList;
}

void Lists::fillInMemListsFromPersistentStore(offset_t nodeOffset,
    uint64_t numElementsInPersistentStore, InMemList& inMemList,
    const std::unordered_set<list_offset_t>& deletedRelOffsetsInList,
    UpdatedPersistentListOffsets* updatedPersistentListOffsets) {
    auto pageMapper = ListHandle::getPageMapper(metadata, nodeOffset);
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        headers->getCSROffset(nodeOffset), numElementsPerPage);
    uint64_t numElementsRead = 0;
    uint64_t nextPosToWriteToInMemList = 0;
    auto numElementsToRead = numElementsInPersistentStore;
    while (numElementsRead < numElementsToRead) {
        auto numElementsToReadInCurPage = std::min(numElementsToRead - numElementsRead,
            (uint64_t)(numElementsPerPage - pageCursor.elemPosInPage));
        auto physicalPageIdx = pageMapper(pageCursor.pageIdx);
        bufferManager->optimisticRead(*fileHandle, physicalPageIdx, [&](uint8_t* frame) {
            fillInMemListsFromFrame(inMemList, frame, pageCursor.elemPosInPage,
                numElementsToReadInCurPage, deletedRelOffsetsInList, numElementsRead,
                nextPosToWriteToInMemList, updatedPersistentListOffsets);
        });
        numElementsRead += numElementsToReadInCurPage;
        pageCursor.nextPage();
    }
}

void Lists::fillInMemListsFromFrame(InMemList& inMemList, const uint8_t* frame,
    uint64_t elemPosInPage, uint64_t numElementsToReadInCurPage,
    const std::unordered_set<uint64_t>& deletedRelOffsetsInList, uint64_t numElementsRead,
    uint64_t& nextPosToWriteToInMemList,
    UpdatedPersistentListOffsets* updatedPersistentListOffsets) {
    auto nullBufferInPage = (uint64_t*)getNullBufferInPage(frame);
    auto frameData = frame + getElemByteOffset(elemPosInPage);
    auto listData = inMemList.getListData() + getElemByteOffset(nextPosToWriteToInMemList);
    // If we don't have any deleted rels, we can do a sequential copy of the entire list from frame
    // to inMemList, and then check whether there are updates to any of the copied rels.
    if (deletedRelOffsetsInList.empty()) {
        memcpy(listData, frameData, numElementsToReadInCurPage * elementSize);
        if (inMemList.hasNullBuffer()) {
            NullMask::copyNullMask(nullBufferInPage, elemPosInPage, inMemList.getNullMask(),
                nextPosToWriteToInMemList, numElementsToReadInCurPage);
        }
        readPropertyUpdatesToInMemListIfExists(inMemList, updatedPersistentListOffsets,
            numElementsRead, numElementsToReadInCurPage, nextPosToWriteToInMemList);
        nextPosToWriteToInMemList += numElementsToReadInCurPage;
    } else {
        // If we have some deleted rels, we should check whether each rel has been deleted or not
        // before copying to inMemList. If not, then we also check whether the rel has been updated.
        for (auto i = 0u; i < numElementsToReadInCurPage; i++) {
            auto relOffsetInList = numElementsRead + i;
            if (!deletedRelOffsetsInList.contains(relOffsetInList)) {
                if (updatedPersistentListOffsets != nullptr &&
                    updatedPersistentListOffsets->listOffsetFTIdxMap.contains(relOffsetInList)) {
                    // If the current rel has updates, we should read from the updateStore.
                    listsUpdatesStore->readPropertyUpdateToInMemList(
                        storageStructureIDAndFName.storageStructureID.listFileID,
                        updatedPersistentListOffsets->listOffsetFTIdxMap.at(relOffsetInList),
                        inMemList, nextPosToWriteToInMemList, dataType,
                        getDiskOverflowFileIfExists());
                } else {
                    // Otherwise, we can directly read from persistentStore.
                    memcpy(listData, frameData, elementSize);
                    if (inMemList.hasNullBuffer()) {
                        NullMask::copyNullMask(nullBufferInPage, elemPosInPage,
                            inMemList.getNullMask(), nextPosToWriteToInMemList,
                            1 /* numBitsToCopy */);
                    }
                }
                listData += elementSize;
                nextPosToWriteToInMemList++;
            }
            frameData += elementSize;
        }
    }
}

void Lists::readPropertyUpdatesToInMemListIfExists(InMemList& inMemList,
    UpdatedPersistentListOffsets* updatedPersistentListOffsets, uint64_t numElementsRead,
    uint64_t numElementsToReadInCurPage, uint64_t nextPosToWriteToInMemList) {
    if (updatedPersistentListOffsets != nullptr) {
        for (auto& [listOffset, ftTupleIdx] : updatedPersistentListOffsets->listOffsetFTIdxMap) {
            if (listOffset < numElementsRead) {
                continue;
            } else if (numElementsRead + numElementsToReadInCurPage <= listOffset) {
                break;
            }
            listsUpdatesStore->readPropertyUpdateToInMemList(
                storageStructureIDAndFName.storageStructureID.listFileID, ftTupleIdx, inMemList,
                nextPosToWriteToInMemList + listOffset - numElementsRead, dataType,
                getDiskOverflowFileIfExists());
        }
    }
}

void ListPropertyLists::readListFromPages(
    ValueVector* valueVector, ListHandle& listHandle, PageElementCursor& pageCursor) {
    uint64_t numValuesToRead = valueVector->state->originalSize;
    uint64_t vectorPos = 0;
    while (vectorPos != numValuesToRead) {
        uint64_t numValuesInPage = numElementsPerPage - pageCursor.elemPosInPage;
        uint64_t numValuesToReadInPage = std::min(numValuesInPage, numValuesToRead - vectorPos);
        auto physicalPageIdx = listHandle.mapper(pageCursor.pageIdx);
        auto [fileHandleToPin, pageIdxToPin] =
            StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
                *fileHandle, physicalPageIdx, *wal, TransactionType::READ_ONLY);
        auto frameBytesOffset = getElemByteOffset(pageCursor.elemPosInPage);
        bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
            auto kuListsToRead = reinterpret_cast<common::ku_list_t*>(frame + frameBytesOffset);
            readNullBitsFromAPage(
                valueVector, frame, pageCursor.elemPosInPage, vectorPos, numValuesToReadInPage);
            for (auto i = 0u; i < numValuesToReadInPage; i++) {
                if (!valueVector->isNull(vectorPos)) {
                    diskOverflowFile.readListToVector(
                        TransactionType::READ_ONLY, kuListsToRead[i], valueVector, vectorPos);
                }
                vectorPos++;
            }
        });
        pageCursor.nextPage();
    }
}

void StringPropertyLists::readFromList(ValueVector* valueVector, ListHandle& listHandle) {
    valueVector->resetAuxiliaryBuffer();
    Lists::readFromList(valueVector, listHandle);
    diskOverflowFile.scanStrings(TransactionType::READ_ONLY, *valueVector);
}

void ListPropertyLists::readFromList(ValueVector* valueVector, ListHandle& listHandle) {
    valueVector->resetAuxiliaryBuffer();
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        headers->getCSROffset(listHandle.getBoundNodeOffset()) + listHandle.getStartElemOffset(),
        numElementsPerPage);
    readListFromPages(valueVector, listHandle, pageCursor);
}

void AdjLists::readValues(
    Transaction* transaction, ValueVector* valueVector, ListHandle& listHandle) {
    valueVector->state->selVector->resetSelectorToUnselected();
    if (listHandle.getListSourceStore() == ListSourceStore::UPDATE_STORE) {
        readFromListsUpdatesStore(listHandle, valueVector);
    } else {
        readFromPersistentStore(listHandle, valueVector);
    }
}

std::unique_ptr<std::vector<nodeID_t>> AdjLists::readAdjacencyListOfNode(
    // We read the adjacency list of a node in 2 steps: i) we read all the bytes from the pages
    // that hold the list into a buffer; and (ii) we interpret the bytes in the buffer based on
    // the nodeIDCompressionScheme into a std::vector of nodeID_t.
    offset_t nodeOffset) {
    auto pageMapper = ListHandle::getPageMapper(metadata, nodeOffset);
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        headers->getCSROffset(nodeOffset), numElementsPerPage);
    // Step 1
    auto numElementsInList = getNumElementsFromListHeader(nodeOffset);
    auto listLenInBytes = numElementsInList * elementSize;
    auto buffer = std::make_unique<uint8_t[]>(listLenInBytes);
    auto sizeLeftToCopy = listLenInBytes;
    auto bufferPtr = buffer.get();
    while (sizeLeftToCopy) {
        auto physicalPageIdx = pageMapper(pageCursor.pageIdx);
        auto sizeToCopyInPage =
            std::min(((uint64_t)(numElementsPerPage - pageCursor.elemPosInPage) * elementSize),
                sizeLeftToCopy);
        bufferManager->optimisticRead(*fileHandle, physicalPageIdx, [&](uint8_t* frame) {
            memcpy(bufferPtr, frame + mapElementPosToByteOffset(pageCursor.elemPosInPage),
                sizeToCopyInPage);
        });
        bufferPtr += sizeToCopyInPage;
        sizeLeftToCopy -= sizeToCopyInPage;
        pageCursor.nextPage();
    }

    // Step 2
    std::unique_ptr<std::vector<nodeID_t>> retVal = std::make_unique<std::vector<nodeID_t>>();
    auto sizeLeftToDecompress = listLenInBytes;
    bufferPtr = buffer.get();
    while (sizeLeftToDecompress) {
        nodeID_t nodeID(0, nbrTableID);
        nodeID.offset = *(offset_t*)bufferPtr;
        bufferPtr += sizeof(offset_t);
        retVal->emplace_back(nodeID);
        sizeLeftToDecompress -= sizeof(offset_t);
    }
    return retVal;
}

// Note: This function sets the original and selected size of the DataChunk into which it will
// read a list of nodes and edges.
void AdjLists::readFromList(ValueVector* valueVector, ListHandle& listHandle) {
    auto startOffsetToRead = listHandle.hasValidRangeToRead() ? listHandle.getEndElemOffset() : 0;
    auto numValuesToRead = std::min(common::DEFAULT_VECTOR_CAPACITY,
        (uint64_t)listHandle.getNumValuesInList() - startOffsetToRead);
    valueVector->state->initOriginalAndSelectedSize(numValuesToRead);
    // We store the updates for adjLists in listsUpdatesStore, however we store the
    // updates for adjColumn in the WAL version of the page. The adjColumn needs to pass a
    // transaction to readNodeIDsBySequentialCopy, so readNodeIDsBySequentialCopy can know
    // whether to read the wal version or the original version of the page. AdjLists never reads
    // the wal version of the page(since its updates are stored in listsUpdatesStore), so we
    // simply pass a dummy read-only transaction to readNodeIDsBySequentialCopy.
    auto dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        headers->getCSROffset(listHandle.getBoundNodeOffset()) + startOffsetToRead,
        numElementsPerPage);
    readInternalIDsBySequentialCopy(dummyReadOnlyTrx.get(), valueVector, pageCursor,
        listHandle.mapper, nbrTableID, true /* hasNoNullGuarantee */);
    // We set the startIdx + numValuesToRead == numValuesInList in listSyncState to indicate to
    // the callers (e.g., the adj_list_extend or var_len_extend) that we have read the small
    // list already. This allows the callers to know when to switch to reading from the update
    // store if there is any updates.
    listHandle.setRangeToRead(startOffsetToRead, numValuesToRead);
}

void AdjLists::readFromListsUpdatesStore(ListHandle& listHandle, ValueVector* valueVector) {
    if (!listHandle.hasValidRangeToRead()) {
        // We have read all values from persistent store or the persistent store is empty, we should
        // reset listSyncState to indicate ranges in listsUpdatesStore and start
        // reading from it.
        listHandle.setRangeToRead(
            0, std::min(DEFAULT_VECTOR_CAPACITY, (uint64_t)listHandle.getNumValuesInList()));
    } else {
        listHandle.setRangeToRead(listHandle.getEndElemOffset(),
            std::min(DEFAULT_VECTOR_CAPACITY,
                (uint64_t)listHandle.getNumValuesInList() - listHandle.getEndElemOffset()));
    }
    // Note that: we always store nbr node in the second column of factorizedTable.
    listsUpdatesStore->readValues(
        storageStructureIDAndFName.storageStructureID.listFileID, listHandle, valueVector);
}

void AdjLists::readFromPersistentStore(ListHandle& listHandle, ValueVector* valueVector) {
    // If the startElemOffset is invalid, it means that we never read from the list. As a
    // result, we need to reset the cursor and mapper.
    if (!listHandle.hasValidRangeToRead()) {
        listHandle.setMapper(metadata);
    }
    readFromList(valueVector, listHandle);
}

// Note: this function will always be called right after scanRelID, so we have the
// guarantee that the relIDVector is always unselected.
void RelIDList::setDeletedRelsIfNecessary(
    Transaction* transaction, ListHandle& listHandle, ValueVector* relIDVector) {
    // We only need to unselect the positions for deleted rels when we are reading from the
    // persistent store in a write transaction and the current nodeOffset has deleted rels in
    // persistent store.
    if (!transaction->isReadOnly() &&
        listHandle.getListSourceStore() != ListSourceStore::UPDATE_STORE &&
        listsUpdatesStore->hasAnyDeletedRelsInPersistentStore(
            storageStructureIDAndFName.storageStructureID.listFileID,
            listHandle.getBoundNodeOffset())) {
        relIDVector->state->selVector->resetSelectorToValuePosBuffer();
        auto& selVector = relIDVector->state->selVector;
        auto nextSelectedPos = 0u;
        for (auto pos = 0; pos < relIDVector->state->originalSize; ++pos) {
            auto relID = relIDVector->getValue<relID_t>(pos);
            if (!listsUpdatesStore->isRelDeletedInPersistentStore(
                    storageStructureIDAndFName.storageStructureID.listFileID,
                    listHandle.getBoundNodeOffset(), relID.offset)) {
                selVector->selectedPositions[nextSelectedPos++] = pos;
            }
        }
        selVector->selectedSize = nextSelectedPos;
    }
}

std::unordered_set<uint64_t> RelIDList::getDeletedRelOffsetsInListForNodeOffset(
    offset_t nodeOffset) {
    std::unordered_set<uint64_t> deletedRelOffsetsInList;
    auto pageMapper = ListHandle::getPageMapper(metadata, nodeOffset);
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        headers->getCSROffset(nodeOffset), numElementsPerPage);
    auto numElementsInPersistentStore = getNumElementsFromListHeader(nodeOffset);
    uint64_t numElementsRead = 0;
    while (numElementsRead < numElementsInPersistentStore) {
        auto numElementsToReadInCurPage = std::min(numElementsInPersistentStore - numElementsRead,
            (uint64_t)(numElementsPerPage - pageCursor.elemPosInPage));
        auto physicalPageIdx = pageMapper(pageCursor.pageIdx);
        bufferManager->optimisticRead(*fileHandle, physicalPageIdx, [&](uint8_t* frame) -> void {
            auto buffer = frame + getElemByteOffset(pageCursor.elemPosInPage);
            for (auto i = 0u; i < numElementsToReadInCurPage; i++) {
                auto relID = *(int64_t*)buffer;
                if (listsUpdatesStore->isRelDeletedInPersistentStore(
                        storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset,
                        relID)) {
                    deletedRelOffsetsInList.emplace(numElementsRead);
                }
                numElementsRead++;
                buffer += elementSize;
            }
        });
        pageCursor.nextPage();
    }
    return deletedRelOffsetsInList;
}

list_offset_t RelIDList::getListOffset(offset_t nodeOffset, offset_t relOffset) {
    auto pageMapper = ListHandle::getPageMapper(metadata, nodeOffset);
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        headers->getCSROffset(nodeOffset), numElementsPerPage);
    auto numElementsInPersistentStore = getNumElementsFromListHeader(nodeOffset);
    uint64_t numElementsRead = 0;
    uint64_t retVal = UINT64_MAX;
    while (numElementsRead < numElementsInPersistentStore && retVal == UINT64_MAX) {
        auto numElementsToReadInCurPage = std::min(numElementsInPersistentStore - numElementsRead,
            (uint64_t)(numElementsPerPage - pageCursor.elemPosInPage));
        auto physicalPageIdx = pageMapper(pageCursor.pageIdx);
        bufferManager->optimisticRead(*fileHandle, physicalPageIdx, [&](uint8_t* frame) -> void {
            auto buffer = frame + getElemByteOffset(pageCursor.elemPosInPage);
            for (auto i = 0u; i < numElementsToReadInCurPage; i++) {
                auto relIDInList = *(int64_t*)buffer;
                if (relIDInList == relOffset) {
                    retVal = numElementsRead;
                    return;
                }
                numElementsRead++;
                buffer += elementSize;
            }
        });
        pageCursor.nextPage();
    }
    // If we don't find the associated listOffset for the given relID in persistent store list,
    // it means that this rel is stored in update store, and we return UINT64_MAX for this case.
    return retVal;
}

void RelIDList::readFromList(ValueVector* valueVector, ListHandle& listHandle) {
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        headers->getCSROffset(listHandle.getBoundNodeOffset()) + listHandle.getStartElemOffset(),
        numElementsPerPage);
    readInternalIDsBySequentialCopy(Transaction::getDummyReadOnlyTrx().get(), valueVector,
        pageCursor, listHandle.mapper, getRelTableID(), true /* hasNoNullGuarantee */);
}

} // namespace storage
} // namespace kuzu
