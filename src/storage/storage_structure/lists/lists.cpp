#include "src/storage/storage_structure/include/lists/lists.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

ListInfo Lists::getListInfo(node_offset_t nodeOffset) {
    ListInfo info;
    auto header = headers->getHeader(nodeOffset);
    if (ListHeaders::isALargeList(header)) {
        info.isLargeList = true;
        auto largeListIdx = ListHeaders::getLargeListIdx(header);
        info.cursor = PageUtils::getPageElementCursorForPos(0, numElementsPerPage);
        info.mapper = metadata.getPageMapperForLargeListIdx(largeListIdx);
        info.listLen = metadata.getNumElementsInLargeLists(largeListIdx);
    } else {
        info.isLargeList = false;
        auto chunkIdx = nodeOffset >> StorageConfig::LISTS_CHUNK_SIZE_LOG_2;
        info.cursor = PageUtils::getPageElementCursorForPos(
            ListHeaders::getSmallListCSROffset(header), numElementsPerPage);
        info.mapper = metadata.getPageMapperForChunkIdx(chunkIdx);
        info.listLen = ListHeaders::getSmallListLen(header);
    }
    return info;
}

// Note: The given nodeOffset and largeListHandle may not be connected. For example if we
// are about to read a new nodeOffset, say v5, after having read a previous nodeOffset, say v7, with
// a largeList, then the input to this function can be nodeOffset: 5 and largeListHandle containing
// information about the last portion of v7's large list. Similarly, if nodeOffset is v3 and v3
// has a small list then largeListHandle does not contain anything specific to v3 (it would likely
// be containing information about the last portion of the last large list that was read).
void Lists::readValues(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<LargeListHandle>& largeListHandle) {
    auto info = getListInfo(nodeOffset);
    if (info.isLargeList) {
        readFromLargeList(valueVector, largeListHandle, info);
    } else {
        readSmallList(valueVector, info);
    }
}

/**
 * Note: This function is called for property lists other than STRINGS. This is called by
 * readValues, which is the main function for reading all lists except UNSTRUCTURED
 * and NODE_ID.
 */
void Lists::readFromLargeList(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info) {
    // assumes that the associated adjList has already updated the syncState.
    auto pageCursor = PageUtils::getPageElementCursorForPos(
        largeListHandle->getListSyncState()->getStartIdx(), numElementsPerPage);
    auto tmpTransaction = make_unique<Transaction>(READ_ONLY, UINT64_MAX);
    readBySequentialCopy(tmpTransaction.get(), valueVector, pageCursor, info.mapper);
}

void Lists::readSmallList(const shared_ptr<ValueVector>& valueVector, ListInfo& info) {
    auto tmpTransaction = make_unique<Transaction>(READ_ONLY, UINT64_MAX);
    readBySequentialCopy(tmpTransaction.get(), valueVector, info.cursor, info.mapper);
}

void StringPropertyLists::readFromLargeList(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info) {
    Lists::readFromLargeList(valueVector, largeListHandle, info);
    stringOverflowPages.readStringsToVector(*valueVector);
}

void StringPropertyLists::readSmallList(
    const shared_ptr<ValueVector>& valueVector, ListInfo& info) {
    Lists::readSmallList(valueVector, info);
    stringOverflowPages.readStringsToVector(*valueVector);
}

void ListPropertyLists::readFromLargeList(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info) {
    Lists::readFromLargeList(valueVector, largeListHandle, info);
    listOverflowPages.readListsToVector(*valueVector);
}

void ListPropertyLists::readSmallList(const shared_ptr<ValueVector>& valueVector, ListInfo& info) {
    Lists::readSmallList(valueVector, info);
    listOverflowPages.readListsToVector(*valueVector);
}

void AdjLists::readFromLargeList(const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info) {
    auto listSyncState = largeListHandle->getListSyncState();
    uint64_t csrOffset;
    if (!largeListHandle->hasMoreToRead()) {
        // initialize listSyncState to start tracking a new list.
        listSyncState->init(info.listLen);
        csrOffset = 0;
    } else {
        csrOffset = listSyncState->getEndIdx();
        info.cursor = PageUtils::getPageElementCursorForPos(csrOffset, numElementsPerPage);
    }
    // The number of edges to read is the minimum of: (i) how may edges are left to read
    // (info.listLen - csrOffset); and (ii) how many elements are left in the current page that's
    // being read (csrOffset above should be set to the beginning of the next page. Note that
    // because of case (ii), this computation guarantees that what we read fits into a single page.
    // That's why we can call copyFromAPage.
    auto numValuesToCopy = min((uint32_t)(info.listLen - csrOffset),
        numElementsPerPage - (uint32_t)(csrOffset % numElementsPerPage));
    valueVector->state->initOriginalAndSelectedSize(numValuesToCopy);
    listSyncState->set(csrOffset, valueVector->state->selVector->selectedSize);
    // map logical pageIdx to physical pageIdx
    auto physicalPageId = info.mapper(info.cursor.pageIdx);
    readNodeIDsFromAPageBySequentialCopy(valueVector, 0, physicalPageId, info.cursor.posInPage,
        numValuesToCopy, nodeIDCompressionScheme, true /*isAdjLists*/);
}

// Note: This function sets the original and selected size of the DataChunk into which it will
// read a list of nodes and edges.
void AdjLists::readSmallList(const shared_ptr<ValueVector>& valueVector, ListInfo& info) {
    valueVector->state->initOriginalAndSelectedSize(info.listLen);
    readNodeIDsBySequentialCopy(
        valueVector, info.cursor, info.mapper, nodeIDCompressionScheme, true /*isAdjLists*/);
}

unique_ptr<vector<nodeID_t>> AdjLists::readAdjacencyListOfNode(
    // We read the adjacency list of a node in 2 steps: i) we read all the bytes from the pages
    // that hold the list into a buffer; and (ii) we interpret the bytes in the buffer based on the
    // ID compression scheme into a vector of nodeID_t.
    node_offset_t nodeOffset) {
    // Step 1
    auto info = getListInfo(nodeOffset);
    auto listLenInBytes = info.listLen * elementSize;
    auto buffer = make_unique<uint8_t[]>(listLenInBytes);
    auto sizeLeftToCopy = listLenInBytes;
    auto bufferPtr = buffer.get();
    while (sizeLeftToCopy) {
        auto physicalPageIdx = info.mapper(info.cursor.pageIdx);
        auto sizeToCopyInPage = min(
            ((uint64_t)(numElementsPerPage - info.cursor.posInPage) * elementSize), sizeLeftToCopy);
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
        memcpy(
            bufferPtr, frame + mapElementPosToByteOffset(info.cursor.posInPage), sizeToCopyInPage);
        bufferManager.unpin(fileHandle, physicalPageIdx);
        bufferPtr += sizeToCopyInPage;
        sizeLeftToCopy -= sizeToCopyInPage;
        info.cursor.posInPage = 0;
        info.cursor.pageIdx++;
    }

    // Step 2
    unique_ptr<vector<nodeID_t>> retVal = make_unique<vector<nodeID_t>>();
    auto sizeLeftToDecompress = listLenInBytes;
    bufferPtr = buffer.get();
    while (sizeLeftToDecompress) {
        nodeID_t nodeID(0, 0);
        memcpy(&nodeID.label, bufferPtr, nodeIDCompressionScheme.getNumBytesForLabel());
        bufferPtr += nodeIDCompressionScheme.getNumBytesForLabel();
        memcpy(&nodeID.offset, bufferPtr, nodeIDCompressionScheme.getNumBytesForOffset());
        bufferPtr += nodeIDCompressionScheme.getNumBytesForOffset();
        retVal->emplace_back(nodeID);
        sizeLeftToDecompress -= nodeIDCompressionScheme.getNumTotalBytes();
    }
    return retVal;
}

} // namespace storage
} // namespace graphflow
