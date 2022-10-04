#include "src/storage/storage_structure/include/lists/lists.h"

#include "src/storage/storage_structure/include/lists/lists_update_iterator.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

void Lists::initListReadingState(
    node_offset_t nodeOffset, ListHandle& listHandle, TransactionType transactionType) {
    auto& listSyncState = listHandle.listSyncState;
    listSyncState.reset();
    listSyncState.setBoundNodeOffset(nodeOffset);
    listSyncState.setListHeader(headers->getHeader(nodeOffset));
    uint64_t numElementsInPersistentStore = getNumElementsInPersistentStore(nodeOffset);
    auto numElementsInUpdateStore =
        transactionType == WRITE ? listUpdateStore->getNumInsertedRelsForNodeOffset(nodeOffset) : 0;
    listSyncState.setNumValuesInList(numElementsInPersistentStore == 0 ?
                                         numElementsInUpdateStore :
                                         numElementsInPersistentStore);
    listSyncState.setDataToReadFromUpdateStore(numElementsInUpdateStore != 0);
    // If there's no element is persistentStore and the relUpdateStore is non-empty, we can
    // skip reading from persistentStore and start reading from relUpdateStore directly.
    listSyncState.setSourceStore(numElementsInPersistentStore == 0 && numElementsInUpdateStore > 0 ?
                                     ListSourceStore::RelUpdateStore :
                                     ListSourceStore::PersistentStore);
}

// Note: The given nodeOffset and largeListHandle may not be connected. For example if we
// are about to read a new nodeOffset, say v5, after having read a previous nodeOffset, say v7, with
// a largeList, then the input to this function can be nodeOffset: 5 and largeListHandle containing
// information about the last portion of v7's large list. Similarly, if nodeOffset is v3 and v3
// has a small list then largeListHandle does not contain anything specific to v3 (it would likely
// be containing information about the last portion of the last large list that was read).
void Lists::readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    auto& listSyncState = listHandle.listSyncState;
    if (listSyncState.getListSourceStore() == ListSourceStore::RelUpdateStore) {
        listUpdateStore->readValues(listSyncState, valueVector,
            listUpdateStore->getColIdxInFT(storageStructureIDAndFName.storageStructureID.listFileID
                                               .relPropertyListID.propertyID));
    } else {
        // If the startElementOffset is 0, it means that this is the first time that we read from
        // the list. As a result, we need to reset the cursor and mapper.
        if (listHandle.listSyncState.getStartElemOffset() == 0) {
            listHandle.resetCursorMapper(metadata, numElementsPerPage);
        }
        readFromList(valueVector, listHandle);
    }
}

void Lists::readSmallList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    auto tmpTransaction = make_unique<Transaction>(READ_ONLY, UINT64_MAX);
    readBySequentialCopy(tmpTransaction.get(), valueVector, listHandle.cursorAndMapper.cursor,
        listHandle.cursorAndMapper.mapper);
}

void Lists::fillListsFromPersistentStore(
    CursorAndMapper& cursorAndMapper, uint64_t numValuesInPersistentStore, uint8_t* dataToFill) {
    uint64_t numBytesRead = 0;
    auto numBytesToRead = numValuesInPersistentStore * elementSize;
    while (numBytesRead < numBytesToRead) {
        auto bytesToReadInCurrentPage = min(numBytesToRead - numBytesRead,
            DEFAULT_PAGE_SIZE - cursorAndMapper.cursor.elemPosInPage * elementSize);
        auto physicalPageIdx = cursorAndMapper.mapper(cursorAndMapper.cursor.pageIdx);
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
        copy(frame + cursorAndMapper.cursor.elemPosInPage * elementSize,
            frame + cursorAndMapper.cursor.elemPosInPage * elementSize + bytesToReadInCurrentPage,
            dataToFill);
        bufferManager.unpin(fileHandle, physicalPageIdx);
        numBytesRead += bytesToReadInCurrentPage;
        dataToFill += bytesToReadInCurrentPage;
        cursorAndMapper.cursor.nextPage();
    }
}

void Lists::readFromList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    if (ListHeaders::isALargeList(listHandle.listSyncState.getListHeader())) {
        readFromLargeList(valueVector, listHandle);
    } else {
        readSmallList(valueVector, listHandle);
    }
}

/**
 * Note: This function is called for property lists other than STRINGS. This is called by
 * readValues, which is the main function for reading all lists except UNSTRUCTURED
 * and NODE_ID.
 */
void Lists::readFromLargeList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    // assumes that the associated adjList has already updated the syncState.
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        listHandle.listSyncState.getStartElemOffset(), numElementsPerPage);
    auto tmpTransaction = make_unique<Transaction>(READ_ONLY, UINT64_MAX);
    readBySequentialCopy(
        tmpTransaction.get(), valueVector, pageCursor, listHandle.cursorAndMapper.mapper);
}

void StringPropertyLists::readFromLargeList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readFromLargeList(valueVector, listHandle);
    stringOverflowPages.readStringsToVector(*valueVector);
}

void StringPropertyLists::readSmallList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readSmallList(valueVector, listHandle);
    stringOverflowPages.readStringsToVector(*valueVector);
}

void ListPropertyLists::readFromLargeList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readFromLargeList(valueVector, listHandle);
    listOverflowPages.readListsToVector(*valueVector);
}

void ListPropertyLists::readSmallList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readSmallList(valueVector, listHandle);
    listOverflowPages.readListsToVector(*valueVector);
}

void AdjLists::readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    auto& listSyncState = listHandle.listSyncState;
    if (listSyncState.getListSourceStore() == ListSourceStore::PersistentStore &&
        listSyncState.getStartElemOffset() + listSyncState.getNumValuesToRead() ==
            listSyncState.getNumValuesInList()) {
        listSyncState.setSourceStore(ListSourceStore::RelUpdateStore);
    }
    if (listSyncState.getListSourceStore() == ListSourceStore::RelUpdateStore) {
        readFromRelUpdateStore(listSyncState, valueVector);
    } else {
        // If the startElemOffset is invalid, it means that we never read from the list. As a
        // result, we need to reset the cursor and mapper.
        if (listHandle.listSyncState.getStartElemOffset() == -1) {
            listHandle.resetCursorMapper(metadata, numElementsPerPage);
        }
        readFromList(valueVector, listHandle);
    }
}

unique_ptr<vector<nodeID_t>> AdjLists::readAdjacencyListOfNode(
    // We read the adjacency list of a node in 2 steps: i) we read all the bytes from the pages
    // that hold the list into a buffer; and (ii) we interpret the bytes in the buffer based on the
    // nodeIDCompressionScheme into a vector of nodeID_t.
    node_offset_t nodeOffset) {
    auto header = headers->getHeader(nodeOffset);
    CursorAndMapper cursorAndMapper;
    cursorAndMapper.reset(getListsMetadata(), numElementsPerPage, header, nodeOffset);
    // Step 1
    auto numElementsInList = getNumElementsInPersistentStore(nodeOffset);
    auto listLenInBytes = numElementsInList * elementSize;
    auto buffer = make_unique<uint8_t[]>(listLenInBytes);
    auto sizeLeftToCopy = listLenInBytes;
    auto bufferPtr = buffer.get();
    while (sizeLeftToCopy) {
        auto physicalPageIdx = cursorAndMapper.mapper(cursorAndMapper.cursor.pageIdx);
        auto sizeToCopyInPage = min(
            ((uint64_t)(numElementsPerPage - cursorAndMapper.cursor.elemPosInPage) * elementSize),
            sizeLeftToCopy);
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
        memcpy(bufferPtr, frame + mapElementPosToByteOffset(cursorAndMapper.cursor.elemPosInPage),
            sizeToCopyInPage);
        bufferManager.unpin(fileHandle, physicalPageIdx);
        bufferPtr += sizeToCopyInPage;
        sizeLeftToCopy -= sizeToCopyInPage;
        cursorAndMapper.cursor.elemPosInPage = 0;
        cursorAndMapper.cursor.pageIdx++;
    }

    // Step 2
    unique_ptr<vector<nodeID_t>> retVal = make_unique<vector<nodeID_t>>();
    auto sizeLeftToDecompress = listLenInBytes;
    bufferPtr = buffer.get();
    while (sizeLeftToDecompress) {
        nodeID_t nodeID(0, 0);
        nodeIDCompressionScheme.readNodeID(bufferPtr, &nodeID);
        bufferPtr += nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression();
        retVal->emplace_back(nodeID);
        sizeLeftToDecompress -= nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression();
    }
    return retVal;
}

void Lists::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    if (listUpdateStore->isEmpty()) {
        return;
    }
    wal->addToUpdatedUnstructuredPropertyLists(
        storageStructureIDAndFName.storageStructureID.listFileID);
    RelPropertyListUpdateIterator updateItr(this);
    if (isCommit) {
        // Note: In C++ iterating through maps happens in non-descending order of the keys. This
        // property is critical when using UnstructuredPropertyListsUpdateIterator, which requires
        // the user to make calls to writeListToListPages in ascending order of nodeOffsets.
        for (auto updatedChunkItr = listUpdateStore->getInsertedEdgeTupleIdxes().begin();
             updatedChunkItr != listUpdateStore->getInsertedEdgeTupleIdxes().end();
             ++updatedChunkItr) {
            for (auto updatedNodeOffsetItr = updatedChunkItr->second.begin();
                 updatedNodeOffsetItr != updatedChunkItr->second.end(); updatedNodeOffsetItr++) {
                auto nodeOffset = updatedNodeOffsetItr->first;
                auto totalNumElements =
                    getTotalNumElementsInList(TransactionType::WRITE, nodeOffset);
                auto newList = make_unique<uint8_t[]>(totalNumElements * elementSize);
                auto nullMask = NullMask();
                CursorAndMapper cursorAndMapper;
                cursorAndMapper.reset(
                    metadata, numElementsPerPage, headers->getHeader(nodeOffset), nodeOffset);
                auto numElementsInPersistentStore = getNumElementsInPersistentStore(nodeOffset);
                fillListsFromPersistentStore(
                    cursorAndMapper, numElementsInPersistentStore, newList.get());
                listUpdateStore->getFactorizedTable()->readToList(
                    listUpdateStore->getColIdxInFT(storageStructureIDAndFName.storageStructureID
                                                       .listFileID.relPropertyListID.propertyID),
                    updatedNodeOffsetItr->second,
                    newList.get() + getNumElementsInPersistentStore(nodeOffset) * elementSize);
                updateItr.updateRelPropertyList(
                    nodeOffset, newList.get(), totalNumElements, nullMask);
            }
        }
    }
    updateItr.doneUpdating();
}

void AdjLists::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    if (listUpdateStore->isEmpty()) {
        return;
    }
    wal->addToUpdatedUnstructuredPropertyLists(
        storageStructureIDAndFName.storageStructureID.listFileID);
    ListsUpdateIterator updateItr(this);
    if (isCommit) {
        // Note: In C++ iterating through maps happens in non-descending order of the keys. This
        // property is critical when using UnstructuredPropertyListsUpdateIterator, which requires
        // the user to make calls to writeListToListPages in ascending order of nodeOffsets.
        for (auto updatedChunkItr = listUpdateStore->getInsertedEdgeTupleIdxes().begin();
             updatedChunkItr != listUpdateStore->getInsertedEdgeTupleIdxes().end();
             ++updatedChunkItr) {
            for (auto updatedNodeOffsetItr = updatedChunkItr->second.begin();
                 updatedNodeOffsetItr != updatedChunkItr->second.end(); updatedNodeOffsetItr++) {
                auto nodeOffset = updatedNodeOffsetItr->first;
                auto totalNumElements =
                    getTotalNumElementsInList(TransactionType::WRITE, nodeOffset);
                auto newList = make_unique<uint8_t[]>(totalNumElements * elementSize);
                CursorAndMapper cursorAndMapper;
                cursorAndMapper.reset(
                    metadata, numElementsPerPage, headers->getHeader(nodeOffset), nodeOffset);
                auto numElementsInPersistentStore = getNumElementsInPersistentStore(nodeOffset);
                fillListsFromPersistentStore(
                    cursorAndMapper, numElementsInPersistentStore, newList.get());
                listUpdateStore->getFactorizedTable()->readToList(1 /* colIdx */,
                    updatedNodeOffsetItr->second,
                    newList.get() + getNumElementsInPersistentStore(nodeOffset) * elementSize);
                updateItr.updateList(nodeOffset, newList.get(), totalNumElements);
            }
        }
    }
    updateItr.doneUpdating();
}

void AdjLists::readFromLargeList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    uint64_t nextPartBeginElemOffset;
    auto& listSyncState = listHandle.listSyncState;
    if (!listSyncState.hasValidRangeToRead()) {
        nextPartBeginElemOffset = 0;
    } else {
        nextPartBeginElemOffset = listSyncState.getEndElemOffset();
        listHandle.cursorAndMapper.cursor =
            PageUtils::getPageElementCursorForPos(nextPartBeginElemOffset, numElementsPerPage);
    }
    // The number of edges to read is the minimum of: (i) how may edges are left to read
    // (info.listLen - nextPartBeginElemOffset); and (ii) how many elements are left in the current
    // page that's being read (nextPartBeginElemOffset above should be set to the beginning of the
    // next page. Note that because of case (ii), this computation guarantees that what we read fits
    // into a single page. That's why we can call copyFromAPage.
    auto numValuesToCopy =
        min((uint32_t)(listSyncState.getNumValuesInList() - nextPartBeginElemOffset),
            numElementsPerPage - (uint32_t)(nextPartBeginElemOffset % numElementsPerPage));
    valueVector->state->initOriginalAndSelectedSize(numValuesToCopy);
    listSyncState.setRangeToRead(
        nextPartBeginElemOffset, valueVector->state->selVector->selectedSize);
    // map logical pageIdx to physical pageIdx
    auto physicalPageId =
        listHandle.cursorAndMapper.mapper(listHandle.cursorAndMapper.cursor.pageIdx);
    readNodeIDsFromAPageBySequentialCopy(valueVector, 0, physicalPageId,
        listHandle.cursorAndMapper.cursor.elemPosInPage, numValuesToCopy, nodeIDCompressionScheme,
        true /*isAdjLists*/);
}

// Note: This function sets the original and selected size of the DataChunk into which it will
// read a list of nodes and edges.
void AdjLists::readSmallList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    valueVector->state->initOriginalAndSelectedSize(listHandle.listSyncState.getNumValuesInList());
    readNodeIDsBySequentialCopy(valueVector, listHandle.cursorAndMapper.cursor,
        listHandle.cursorAndMapper.mapper, nodeIDCompressionScheme, true /*isAdjLists*/);
    // We set the startIdx + numValuesToRead == numValuesInList in listSyncState to indicate to the
    // callers (e.g., the adj_list_extend or var_len_extend) that we have read the small list
    // already. This allows the callers to know when to switch to reading from the update store if
    // there is any updates.
    listHandle.listSyncState.setRangeToRead(0, listHandle.listSyncState.getNumValuesInList());
}

void AdjLists::readFromRelUpdateStore(
    ListSyncState& listSyncState, shared_ptr<ValueVector> valueVector) const {
    if (listSyncState.getStartElemOffset() + listSyncState.getNumValuesToRead() ==
            listSyncState.getNumValuesInList() ||
        !listSyncState.hasValidRangeToRead()) {
        // We have read all values from persistent store or the persistent store is empty, we should
        // reset listSyncState to indicate ranges in relUpdateStore and start reading from
        // relUpdateStore.
        listSyncState.setNumValuesInList(
            listUpdateStore->getNumInsertedRelsForNodeOffset(listSyncState.getBoundNodeOffset()));
        listSyncState.setRangeToRead(
            0, min(DEFAULT_VECTOR_CAPACITY, listSyncState.getNumValuesInList()));
    } else {
        listSyncState.setRangeToRead(listSyncState.getEndElemOffset(),
            min(DEFAULT_VECTOR_CAPACITY,
                listSyncState.getNumValuesInList() - listSyncState.getEndElemOffset()));
    }
    // Note that: we always store nbr node in the second column of factorizedTable.
    listUpdateStore->readValues(listSyncState, valueVector, 1 /* colIdx */);
}

} // namespace storage
} // namespace graphflow
