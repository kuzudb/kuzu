#include "storage/storage_structure/lists/lists.h"

#include "storage/storage_structure/lists/lists_update_iterator.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void Lists::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    auto updateItr = ListsUpdateIteratorFactory::getListsUpdateIterator(this);
    if (isCommit) {
        prepareCommit(*updateItr);
    }
    updateItr->doneUpdating();
}

// Note: The given nodeOffset and largeListHandle may not be connected. For example if we
// are about to read a new nodeOffset, say v5, after having read a previous nodeOffset, say v7, with
// a largeList, then the input to this function can be nodeOffset: 5 and largeListHandle containing
// information about the last portion of v7's large list. Similarly, if nodeOffset is v3 and v3
// has a small list then largeListHandle does not contain anything specific to v3 (it would likely
// be containing information about the last portion of the last large list that was read).
void Lists::readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    auto& listSyncState = listHandle.listSyncState;
    if (listSyncState.getListSourceStore() == ListSourceStore::ListsUpdateStore) {
        listsUpdateStore->readValues(
            storageStructureIDAndFName.storageStructureID.listFileID, listSyncState, valueVector);
    } else {
        // If the startElementOffset is 0, it means that this is the first time that we read from
        // the list. As a result, we need to reset the cursor and mapper.
        if (listHandle.listSyncState.getStartElemOffset() == 0) {
            listHandle.resetCursorMapper(metadata, numElementsPerPage);
        }
        readFromList(valueVector, listHandle);
    }
}

void Lists::readFromSmallList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    auto dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    readBySequentialCopy(dummyReadOnlyTrx.get(), valueVector, listHandle.cursorAndMapper.cursor,
        listHandle.cursorAndMapper.mapper);
}

void Lists::readFromLargeList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    // Assumes that the associated adjList has already updated the syncState.
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        listHandle.listSyncState.getStartElemOffset(), numElementsPerPage);
    auto dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    readBySequentialCopy(
        dummyReadOnlyTrx.get(), valueVector, pageCursor, listHandle.cursorAndMapper.mapper);
}

void Lists::readFromList(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    if (ListHeaders::isALargeList(listHandle.listSyncState.getListHeader())) {
        readFromLargeList(valueVector, listHandle);
    } else {
        readFromSmallList(valueVector, listHandle);
    }
}

void Lists::fillInMemListsFromPersistentStore(
    CursorAndMapper& cursorAndMapper, uint64_t numElementsInPersistentStore, InMemList& inMemList) {
    uint64_t numElementsRead = 0;
    auto numElementsToRead = numElementsInPersistentStore;
    auto listData = inMemList.getListData();
    while (numElementsRead < numElementsToRead) {
        auto numElementsToReadInCurPage = min(numElementsToRead - numElementsRead,
            (uint64_t)(numElementsPerPage - cursorAndMapper.cursor.elemPosInPage));
        auto physicalPageIdx = cursorAndMapper.mapper(cursorAndMapper.cursor.pageIdx);
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
        memcpy(listData, frame + cursorAndMapper.cursor.elemPosInPage * elementSize,
            numElementsToReadInCurPage * elementSize);
        if (inMemList.hasNullBuffer()) {
            NullMask::copyNullMask((uint64_t*)(frame + numElementsPerPage * elementSize),
                cursorAndMapper.cursor.elemPosInPage, inMemList.getNullMask(), numElementsRead,
                numElementsToReadInCurPage);
        }
        bufferManager.unpin(fileHandle, physicalPageIdx);
        numElementsRead += numElementsToReadInCurPage;
        listData += numElementsToReadInCurPage * elementSize;
        cursorAndMapper.cursor.nextPage();
    }
}

uint64_t Lists::getNumElementsInPersistentStore(
    TransactionType transactionType, node_offset_t nodeOffset) {
    if (transactionType == TransactionType::WRITE &&
        listsUpdateStore->isListEmptyInPersistentStore(
            storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset)) {
        return 0;
    }
    return getNumElementsFromListHeader(nodeOffset);
}

void Lists::initListReadingState(
    node_offset_t nodeOffset, ListHandle& listHandle, TransactionType transactionType) {
    auto& listSyncState = listHandle.listSyncState;
    listSyncState.reset();
    listSyncState.setBoundNodeOffset(nodeOffset);
    auto isListEmptyInPersistentStore = listsUpdateStore->isListEmptyInPersistentStore(
        storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset);
    if (transactionType == TransactionType::READ_ONLY || !isListEmptyInPersistentStore) {
        listSyncState.setListHeader(headers->getHeader(nodeOffset));
    } else {
        // ListHeader is UINT32_MAX in two cases: (i) ListSyncState is not initialized; or (ii) the
        // list of a new node is being scanned so there is no header for the new node.
        listSyncState.setListHeader(UINT32_MAX);
    }
    auto numElementsInPersistentStore =
        getNumElementsInPersistentStore(transactionType, nodeOffset);
    auto numElementsInUpdateStore =
        transactionType == WRITE ?
            listsUpdateStore->getNumInsertedRelsForNodeOffset(
                storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset) :
            0;
    listSyncState.setNumValuesInList(numElementsInPersistentStore == 0 ?
                                         numElementsInUpdateStore :
                                         numElementsInPersistentStore);
    listSyncState.setDataToReadFromUpdateStore(numElementsInUpdateStore != 0);
    // If there's no element is persistentStore and the listsUpdateStore is non-empty,
    // we can skip reading from persistentStore and start reading from listsUpdateStore directly.
    listSyncState.setSourceStore(
        ((numElementsInPersistentStore == 0 && numElementsInUpdateStore > 0) ||
            isListEmptyInPersistentStore) ?
            ListSourceStore::ListsUpdateStore :
            ListSourceStore::PersistentStore);
}

unique_ptr<InMemList> Lists::getInMemListWithDataFromUpdateStoreOnly(
    node_offset_t nodeOffset, vector<uint64_t>& insertedRelsTupleIdxInFT) {
    auto inMemList = make_unique<InMemList>(
        getNumElementsInListsUpdateStore(nodeOffset), elementSize, mayContainNulls());
    listsUpdateStore->readInsertionsToList(storageStructureIDAndFName.storageStructureID.listFileID,
        insertedRelsTupleIdxInFT, *inMemList, 0 /* numElementsInPersistentStore */,
        getDiskOverflowFileIfExists(), dataType, getNodeIDCompressionIfExists());
    return inMemList;
}

void Lists::prepareCommit(ListsUpdateIterator& listsUpdateIterator) {
    // Note: In C++ iterating through maps happens in non-descending order of the keys. This
    // property is critical when using listsUpdateIterator, which requires
    // the user to make calls to writeInMemListToListPages in ascending order of nodeOffsets.
    auto& listUpdatesPerChunk = listsUpdateStore->getListUpdatesPerChunk(
        storageStructureIDAndFName.storageStructureID.listFileID);
    for (auto updatedChunkItr = listUpdatesPerChunk.begin();
         updatedChunkItr != listUpdatesPerChunk.end(); ++updatedChunkItr) {
        for (auto updatedNodeOffsetItr = updatedChunkItr->second.begin();
             updatedNodeOffsetItr != updatedChunkItr->second.end(); updatedNodeOffsetItr++) {
            auto nodeOffset = updatedNodeOffsetItr->first;
            if (listsUpdateStore->isListEmptyInPersistentStore(
                    storageStructureIDAndFName.storageStructureID.listFileID, nodeOffset)) {
                listsUpdateIterator.updateList(
                    nodeOffset, *getInMemListWithDataFromUpdateStoreOnly(nodeOffset,
                                    updatedNodeOffsetItr->second.insertedRelsTupleIdxInFT));
            } else if (ListHeaders::isALargeList(headers->headersDiskArray->get(
                           nodeOffset, TransactionType::READ_ONLY))) {
                // We do an optimization for relPropertyList and adjList:
                // If the initial list is a largeList, we can simply append the data from the
                // relUpdateStore to largeList. In this case, we can skip reading the data from
                // persistentStore to InMemList and only need to read the data from relUpdateStore
                // to InMemList.
                listsUpdateIterator.appendToLargeList(
                    nodeOffset, *getInMemListWithDataFromUpdateStoreOnly(nodeOffset,
                                    updatedNodeOffsetItr->second.insertedRelsTupleIdxInFT));
            } else {
                auto inMemList = make_unique<InMemList>(
                    getTotalNumElementsInList(TransactionType::WRITE, nodeOffset), elementSize,
                    mayContainNulls());
                CursorAndMapper cursorAndMapper;
                cursorAndMapper.reset(
                    metadata, numElementsPerPage, headers->getHeader(nodeOffset), nodeOffset);
                auto numElementsInPersistentStore = getNumElementsFromListHeader(nodeOffset);
                fillInMemListsFromPersistentStore(
                    cursorAndMapper, numElementsInPersistentStore, *inMemList);
                listsUpdateStore->readInsertionsToList(
                    storageStructureIDAndFName.storageStructureID.listFileID,
                    updatedNodeOffsetItr->second.insertedRelsTupleIdxInFT, *inMemList,
                    numElementsInPersistentStore, getDiskOverflowFileIfExists(), dataType,
                    getNodeIDCompressionIfExists());
                listsUpdateIterator.updateList(nodeOffset, *inMemList);
            }
        }
    }
}

void StringPropertyLists::readFromLargeList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readFromLargeList(valueVector, listHandle);
    diskOverflowFile.readStringsToVector(TransactionType::READ_ONLY, *valueVector);
}

void StringPropertyLists::readFromSmallList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readFromSmallList(valueVector, listHandle);
    diskOverflowFile.readStringsToVector(TransactionType::READ_ONLY, *valueVector);
}

void ListPropertyLists::readFromLargeList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readFromLargeList(valueVector, listHandle);
    diskOverflowFile.readListsToVector(TransactionType::READ_ONLY, *valueVector);
}

void ListPropertyLists::readFromSmallList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    Lists::readFromSmallList(valueVector, listHandle);
    diskOverflowFile.readListsToVector(TransactionType::READ_ONLY, *valueVector);
}

void AdjLists::readValues(const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    auto& listSyncState = listHandle.listSyncState;
    if (listSyncState.getListSourceStore() == ListSourceStore::PersistentStore &&
        listSyncState.getStartElemOffset() + listSyncState.getNumValuesToRead() ==
            listSyncState.getNumValuesInList()) {
        listSyncState.setSourceStore(ListSourceStore::ListsUpdateStore);
    }
    if (listSyncState.getListSourceStore() == ListSourceStore::ListsUpdateStore) {
        readFromListsUpdateStore(listSyncState, valueVector);
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
    auto numElementsInList = getNumElementsFromListHeader(nodeOffset);
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
    // See comments for AdjLists::readFromSmallList.
    auto dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    readNodeIDsFromAPageBySequentialCopy(dummyReadOnlyTrx.get(), valueVector, 0, physicalPageId,
        listHandle.cursorAndMapper.cursor.elemPosInPage, numValuesToCopy, nodeIDCompressionScheme,
        true /*isAdjLists*/);
}

// Note: This function sets the original and selected size of the DataChunk into which it will
// read a list of nodes and edges.
void AdjLists::readFromSmallList(
    const shared_ptr<ValueVector>& valueVector, ListHandle& listHandle) {
    valueVector->state->initOriginalAndSelectedSize(listHandle.listSyncState.getNumValuesInList());
    // We store the updates for adjLists in listsUpdateStore, however we store the
    // updates for adjColumn in the WAL version of the page. The adjColumn needs to pass a
    // transaction to readNodeIDsBySequentialCopy, so readNodeIDsBySequentialCopy can know whether
    // to read the wal version or the original version of the page. AdjLists never reads the wal
    // version of the page(since its updates are stored in listsUpdateStore), so we
    // simply pass a dummy read-only transaction to readNodeIDsBySequentialCopy.
    auto dummyReadOnlyTrx = Transaction::getDummyReadOnlyTrx();
    readNodeIDsBySequentialCopy(dummyReadOnlyTrx.get(), valueVector,
        listHandle.cursorAndMapper.cursor, listHandle.cursorAndMapper.mapper,
        nodeIDCompressionScheme, true /*isAdjLists*/);
    // We set the startIdx + numValuesToRead == numValuesInList in listSyncState to indicate to the
    // callers (e.g., the adj_list_extend or var_len_extend) that we have read the small list
    // already. This allows the callers to know when to switch to reading from the update store if
    // there is any updates.
    listHandle.listSyncState.setRangeToRead(0, listHandle.listSyncState.getNumValuesInList());
}

void AdjLists::readFromListsUpdateStore(
    ListSyncState& listSyncState, shared_ptr<ValueVector> valueVector) {
    if (listSyncState.getStartElemOffset() + listSyncState.getNumValuesToRead() ==
            listSyncState.getNumValuesInList() ||
        !listSyncState.hasValidRangeToRead()) {
        // We have read all values from persistent store or the persistent store is empty, we should
        // reset listSyncState to indicate ranges in listsUpdateStore and start
        // reading from it.
        listSyncState.setNumValuesInList(listsUpdateStore->getNumInsertedRelsForNodeOffset(
            storageStructureIDAndFName.storageStructureID.listFileID,
            listSyncState.getBoundNodeOffset()));
        listSyncState.setRangeToRead(
            0, min(DEFAULT_VECTOR_CAPACITY, listSyncState.getNumValuesInList()));
    } else {
        listSyncState.setRangeToRead(listSyncState.getEndElemOffset(),
            min(DEFAULT_VECTOR_CAPACITY,
                listSyncState.getNumValuesInList() - listSyncState.getEndElemOffset()));
    }
    // Note that: we always store nbr node in the second column of factorizedTable.
    listsUpdateStore->readValues(
        storageStructureIDAndFName.storageStructureID.listFileID, listSyncState, valueVector);
}

} // namespace storage
} // namespace kuzu
