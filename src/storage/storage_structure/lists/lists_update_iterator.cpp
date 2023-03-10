#include "storage/storage_structure/lists/lists_update_iterator.h"

#include "storage/storage_structure/storage_structure_utils.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void ListsUpdateIterator::updateList(offset_t nodeOffset, InMemList& inMemList) {
    seekToNodeOffsetAndSlideListsIfNecessary(nodeOffset);
    list_header_t oldHeader;
    if (nodeOffset >=
        lists->getHeaders()->headersDiskArray->getNumElements(TransactionType::READ_ONLY)) {
        oldHeader = ListHeaders::getSmallListHeader(
            UINT32_MAX /* csrOffset */, UINT32_MAX /* numElementsInList */);
        // If this is a newly inserted node, we should insert a dummy value to the
        // listHeader. Note: the updateLargeList or updateSmallListAndCurCSROffset is
        // responsible for correctly updating the header.
        if (nodeOffset ==
            lists->getHeaders()->headersDiskArray->getNumElements(TransactionType::WRITE)) {
            lists->getHeaders()->headersDiskArray->pushBack(oldHeader);
        }
    } else {
        oldHeader = lists->getHeaders()->headersDiskArray->get(
            curUnprocessedNodeOffset, TransactionType::READ_ONLY);
    }
    if (isLargeListAfterInsertion(oldHeader, inMemList.numElements)) {
        updateLargeList(oldHeader, inMemList);
    } else {
        updateSmallListAndCurCSROffset(oldHeader, inMemList);
    }
    curUnprocessedNodeOffset++;
}

// If the initial list is a largeList, we can simply append the data in inMemList to the
// largeList.
void ListsUpdateIterator::appendToLargeList(offset_t nodeOffset, InMemList& inMemList) {
    seekToNodeOffsetAndSlideListsIfNecessary(nodeOffset);
    auto largeListIdx = ListHeaders::getLargeListIdx(
        lists->headers->headersDiskArray->get(nodeOffset, TransactionType::READ_ONLY));
    auto numElementsInPersistentStore = lists->getNumElementsFromListHeader(nodeOffset);
    lists->getListsMetadata().largeListIdxToPageListHeadIdxMap->update(
        2 * largeListIdx + 1, inMemList.numElements + numElementsInPersistentStore);
    auto idInPageGroupAndOffsetInListPage =
        StorageUtils::getQuotientRemainder(numElementsInPersistentStore, lists->numElementsPerPage);
    writeAtOffset(inMemList,
        (*lists->getListsMetadata().largeListIdxToPageListHeadIdxMap)[2 * largeListIdx],
        idInPageGroupAndOffsetInListPage.first, idInPageGroupAndOffsetInListPage.second);
    curUnprocessedNodeOffset++;
}

void ListsUpdateIterator::doneUpdating() {
    if (curChunkIdx != UINT64_MAX) {
        // If we are done updating, we should seek to the end of the curChunkIdx. However
        // curChunkIdx may be the last chunk and the maximum node offset may be smaller than the
        // maximum nodeOffset that would be in curChunkIdx if the curChunkIdx was full (which is
        // what is returned by getChunkIdxEndNodeOffsetInclusive(curChunkIdx)). So we take the
        // minimum of max Note also that we are trying to seek to the nodeOffset to the
        // immediate right of the last node we want to slide. We do not have a "+ 1" for
        // headersDiskArray->getNumElements, because that is exactly + 1 the last nodeOffset we
        // want to slide. We have a + 1 for getChunkIdxEndNodeOffsetInclusive(curChunkIdx)
        // because we want to slide the node with offset
        // getChunkIdxEndNodeOffsetInclusive(curChunkIdx).
        auto endNodeOffsetToSeekTo =
            std::min(lists->getHeaders()->headersDiskArray->getNumElements(TransactionType::WRITE),
                StorageUtils::getChunkIdxEndNodeOffsetInclusive(curChunkIdx) + 1);
        seekToNodeOffsetAndSlideListsIfNecessary(endNodeOffsetToSeekTo);
    }
    finishCalled = true;
}

void ListsUpdateIterator::seekToBeginningOfChunkIdx(uint64_t chunkIdx) {
    curChunkIdx = chunkIdx;
    curUnprocessedNodeOffset = StorageUtils::getChunkIdxBeginNodeOffset(curChunkIdx);
    curCSROffset = 0;
}

void ListsUpdateIterator::slideListsIfNecessary(uint64_t endNodeOffsetInclusive) {
    for (offset_t nodeOffsetToSlide = curUnprocessedNodeOffset;
         nodeOffsetToSlide <= endNodeOffsetInclusive; ++nodeOffsetToSlide) {
        list_header_t oldHeader = lists->getHeaders()->headersDiskArray->get(
            nodeOffsetToSlide, TransactionType::READ_ONLY);
        // Because large lists get their own set of pages and are not part of the CSR maintained
        // in the current chunk's page list, we do not need to slide them. Only small lists need
        // to be slided if necessary. The necessity condition is if the old header is not equal
        // to the new header that would be formed by the current CSR offset.
        if (!ListHeaders::isALargeList(oldHeader)) {
            auto [listLen, csrOffset] = ListHeaders::getSmallListLenAndCSROffset(oldHeader);
            list_header_t newHeader = ListHeaders::getSmallListHeader(curCSROffset, listLen);
            if (newHeader != oldHeader) {
                InMemList inMemList{listLen, lists->elementSize, lists->mayContainNulls()};
                const std::unordered_set<uint64_t> deletedRelOffsetsInList;
                lists->fillInMemListsFromPersistentStore(nodeOffsetToSlide,
                    lists->getNumElementsFromListHeader(nodeOffsetToSlide), inMemList,
                    deletedRelOffsetsInList);
                updateSmallListAndCurCSROffset(oldHeader, inMemList);
            } else {
                curCSROffset += listLen;
            }
        }
        curUnprocessedNodeOffset++;
    }
}

void ListsUpdateIterator::seekToNodeOffsetAndSlideListsIfNecessary(offset_t nodeOffsetToSeekTo) {
    auto chunkIdxOfNode = StorageUtils::getListChunkIdx(nodeOffsetToSeekTo);
    if (curChunkIdx == UINT64_MAX) {
        seekToBeginningOfChunkIdx(chunkIdxOfNode);
    } else if (curChunkIdx != chunkIdxOfNode) {
        // If the next node offset whose list will be updated is in another chunk, and if
        // curChunkIdx was an actual chunkIdx (i.e., it was not null=UINT64_MAX), we first slide
        // (if necessary) any lists from the curUnprocessedNodeOffset (inclusive) to the last
        // node offset in the current chunkIdx (inclusive)
        slideListsIfNecessary(StorageUtils::getChunkIdxEndNodeOffsetInclusive(curChunkIdx));
        seekToBeginningOfChunkIdx(chunkIdxOfNode);
    }
    // At this point we are guaranteed to have seeked to a node offset that is the same chunkIdx
    // as the nodeOffsetToSeekTo. So we first slide (if necessary) all lists until
    // nodeOffsetToSeekTo - 1 (inclusive). This will ensure that the curUnprocessedNodeOffset is
    // nodeOffsetToSeekTo, so the writeInMemListToListPages function can update the list of
    // nodeOffsetToSeekTo.
    if (nodeOffsetToSeekTo > 0) {
        slideListsIfNecessary(nodeOffsetToSeekTo - 1);
    }
}

void ListsUpdateIterator::writeInMemListToListPages(
    InMemList& inMemList, page_idx_t pageListHeadIdx, bool isSmallList) {
    uint64_t idxInPageList;
    uint64_t elementOffsetInListPage;
    if (isSmallList) {
        std::pair<uint64_t, uint64_t> idInPageGroupAndOffsetInListPage =
            StorageUtils::getQuotientRemainder(curCSROffset, lists->numElementsPerPage);
        idxInPageList = idInPageGroupAndOffsetInListPage.first;
        elementOffsetInListPage = idInPageGroupAndOffsetInListPage.second;
    } else {
        // For large lists, the firstIdxInPageGroup is always 0.
        idxInPageList = 0;
        elementOffsetInListPage = 0;
    }
    writeAtOffset(inMemList, pageListHeadIdx, idxInPageList, elementOffsetInListPage);
}

void ListsUpdateIterator::updateLargeList(list_header_t oldHeader, InMemList& inMemList) {
    page_idx_t pageListHeadIdx;
    uint32_t largeListIdx;
    if (ListHeaders::isALargeList(oldHeader)) {
        largeListIdx = ListHeaders::getLargeListIdx(oldHeader);
        // Note that we can actually directly read from the original
        // largeListIdxToPageListHeadIdxMap here since this listUpdateIterator should be called
        // only once when prepareToCommit on lists is called, and we should be updating each
        // node's list at most once. Therefore the read and write version of the values for
        // pageListHeadIdx for the curUnprocessedNodeOffset should be the same.
        pageListHeadIdx =
            (*lists->getListsMetadata().largeListIdxToPageListHeadIdxMap)[2 * largeListIdx];
    } else {
        // 1) We need to first obtain a largeListIdx. To do this, since each largeIdx occupies
        // 2 slots in the largeListIdxToPageListHeadIdxMap, we get the latest num elements in
        // largeListIdxToPageListHeadIdxMap and divide it by 2.
        largeListIdx = lists->getListsMetadata().largeListIdxToPageListHeadIdxMap->getNumElements(
                           TransactionType::WRITE) >>
                       1;
        // 2) We only need to update list header when this is an
        // adjListsUpdateIterator because relPropertyList share the same
        // header with adjList. The below updateLargeListHeaderIfNecessary(...) function will
        // only update the header if the executing class is adjListsUpdateIterator.
        updateLargeListHeaderIfNecessary(largeListIdx);
        // 3) Then we set a NULL pageListHeadIdx and also NULL for numElements of the new large
        // list. The call to insertNewPageGroupAndSetHeadIdxMap will update the pageListHeadIdx.
        // And after the else branch we will insert the numElements.
        lists->getListsMetadata().largeListIdxToPageListHeadIdxMap->pushBack(
            StorageStructureUtils::NULL_CHUNK_OR_LARGE_LIST_HEAD_IDX);
        lists->getListsMetadata().largeListIdxToPageListHeadIdxMap->pushBack(UINT32_MAX);
        // 4) Finally, we insert the initial page group into pageLists for the new large list
        pageListHeadIdx = insertNewPageGroupAndSetHeadIdxMap(
            UINT32_MAX /* no curIdxInPageList; update largeListIdxToPageListHeadIdxMap */,
            largeListIdx);
    }
    // Note that for largeListIdx, 2*largeListIdx and 2*largeListIdx + 1 are, respectively, the
    // indices in largeListIdxToPageListHeadIdxMap of where the pageListHeadIdx and
    // numElements for the large list are stored.
    lists->getListsMetadata().largeListIdxToPageListHeadIdxMap->update(
        2 * largeListIdx + 1, inMemList.numElements);
    writeInMemListToListPages(inMemList, pageListHeadIdx, false /* isSmallList */);
}

void ListsUpdateIterator::updateSmallListAndCurCSROffset(
    list_header_t oldHeader, InMemList& inMemList) {
    // Note that we do not need to differentiate whether this function is running for
    // AdjLists (which need to compute newHeaders and actually update them) vs RelPropertyLists
    // (which do not update headers and can always read new headers from the updated version of
    // the header). This is because AdjList and RelPropertyLists are parallel and if a list
    // remained small then, we would iteratively always compute the same new csrOffsets for each
    // small list header.
    list_header_t newHeader = ListHeaders::getSmallListHeader(curCSROffset, inMemList.numElements);
    // 1: Only the adjListsUpdateIterator need to update the header, so the below
    // updateSmallListHeaderIfNecessary(...) will only update the header if the executing class
    // is adjListsUpdateIterator.
    updateSmallListHeaderIfNecessary(oldHeader, newHeader);

    // 2: Find the pageListHeadIdx for the chunk. If this chunk is "new", i.e., a new list was
    // inserted that created this chunk, then insert a NULL chunk head Idx.
    page_idx_t pageListHeadIdx;
    if (curChunkIdx == lists->getListsMetadata().chunkToPageListHeadIdxMap->getNumElements(
                           TransactionType::WRITE)) {
        // We need to add a new empty chunkIdx;
        pageListHeadIdx = StorageStructureUtils::NULL_CHUNK_OR_LARGE_LIST_HEAD_IDX;
        lists->getListsMetadata().chunkToPageListHeadIdxMap->pushBack(pageListHeadIdx);
    } else {
        pageListHeadIdx = lists->getListsMetadata().chunkToPageListHeadIdxMap->get(
            curChunkIdx, TransactionType::WRITE);
    }

    // If the newList is empty we can return. Changing the header is enough.
    if (inMemList.numElements <= 0) {
        return;
    }

    // If the chunk head idx is null, insert an initial page group for the chunk.
    if (pageListHeadIdx == StorageStructureUtils::NULL_CHUNK_OR_LARGE_LIST_HEAD_IDX) {
        // Note that for small lists we don't pass the second argument (the largeListIdx) to
        // insertNewPageGroupAndSetHeadIdxMap, which is passed automatically as null=UINT32_MAX.
        pageListHeadIdx = insertNewPageGroupAndSetHeadIdxMap(
            UINT32_MAX /* no curIdxInPageList; update chunkToPageListHeadIdxMap */);
    }

    // 3: Write the newList to the list pages.
    writeInMemListToListPages(inMemList, pageListHeadIdx, true /* is small list */);
    // 4: Update the curCSROffset
    curCSROffset += inMemList.numElements;
}

std::pair<page_idx_t, bool>
ListsUpdateIterator::findListPageIdxAndInsertListPageToPageListIfNecessary(
    page_idx_t idxInPageList, uint32_t pageListHeadIdx) {
    auto pageLists = lists->getListsMetadata().pageLists.get();
    uint32_t curIdxInPageList = pageListHeadIdx;
    while (ListsMetadataConstants::PAGE_LIST_GROUP_SIZE <= idxInPageList) {
        auto nextIdxInPageList =
            pageLists->get(curIdxInPageList + ListsMetadataConstants::PAGE_LIST_GROUP_SIZE,
                TransactionType::WRITE);
        if (nextIdxInPageList == StorageStructureUtils::NULL_PAGE_IDX) {
            // If idxInPageList >= ListsMetadataConstants::PAGE_LIST_GROUP_SIZE but the
            // current page group does not have a new page group, it must be the case that
            // the idxInPageList is the first page in the next page group. This is because we
            // are consecutively updating the lists. suppose next list needs to be put in
            // page x in the page list, i.e., the idxInPageList for the next list is x, but
            // currently, the page group to hold x does not exist, then it has to be the case
            // that there are page groups (and list pages) for up to page x-1, and not for x,
            // x+1, and x+2 (assuming PAGE_GROUP_SIZE is 3). For example x can be 3, and we may
            // have only 1 page group. But x cannot be 4 and we ran out of page groups because
            // the previous list we stored would have created the page group for 3, 4, and 5
            // (note the first page group would store x 0, 1, and 2).
            assert(idxInPageList == ListsMetadataConstants::PAGE_LIST_GROUP_SIZE);
            curIdxInPageList = insertNewPageGroupAndSetHeadIdxMap(curIdxInPageList);
        } else {
            curIdxInPageList = nextIdxInPageList;
        }
        idxInPageList -= ListsMetadataConstants::PAGE_LIST_GROUP_SIZE;
    }
    curIdxInPageList += idxInPageList;
    page_idx_t listPageIdx = pageLists->get(curIdxInPageList, TransactionType::WRITE);
    bool isInsertingNewListPage = false;
    if (listPageIdx == StorageStructureUtils::NULL_PAGE_IDX) {
        isInsertingNewListPage = true;
        listPageIdx = lists->getFileHandle()->addNewPage();
        pageLists->update(curIdxInPageList, listPageIdx);
    }
    return std::make_pair(listPageIdx, isInsertingNewListPage);
}

page_idx_t ListsUpdateIterator::insertNewPageGroupAndSetHeadIdxMap(
    uint32_t beginIdxOfCurrentPageGroup, uint32_t largeListIdx) {
    auto pageLists = lists->getListsMetadata().pageLists.get();
    auto beginIdxOfNextPageGroup = pageLists->getNumElements(TransactionType::WRITE);
    // If the beginIdxOfCurrentPageGroup is null (identified as UINT32_MAX) then we update the
    // list header.
    if (beginIdxOfCurrentPageGroup == UINT32_MAX) {
        if (largeListIdx == UINT32_MAX) {
            lists->getListsMetadata().chunkToPageListHeadIdxMap->update(
                curChunkIdx, (uint32_t)beginIdxOfNextPageGroup);
        } else {
            lists->getListsMetadata().largeListIdxToPageListHeadIdxMap->update(
                2 * largeListIdx, (uint32_t)beginIdxOfNextPageGroup);
        }
    } else {
        // Update the next pointer of the current page group.
        pageLists->update(beginIdxOfCurrentPageGroup + ListsMetadataConstants::PAGE_LIST_GROUP_SIZE,
            beginIdxOfNextPageGroup);
    }
    for (int i = 0; i < ListsMetadataConstants::PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE; ++i) {
        pageLists->pushBack(StorageStructureUtils::NULL_PAGE_IDX);
    }
    return beginIdxOfNextPageGroup;
}

void ListsUpdateIterator::writeAtOffset(InMemList& inMemList, page_idx_t pageListHeadIdx,
    uint64_t idxInPageList, uint64_t elementOffsetInListPage) {
    uint64_t elementSize = lists->elementSize;
    auto remainingNumElementsToWrite = inMemList.numElements;
    uint64_t numUpdatedElements = 0;
    bool firstIteration = true;
    while (remainingNumElementsToWrite > 0) {
        if (firstIteration) {
            firstIteration = false;
        } else {
            idxInPageList++;
            elementOffsetInListPage = 0;
        }
        // Now find the physical pageIdx that this corresponds to the logical idxInPageList
        auto [listPageIdx, insertingNewPage] =
            findListPageIdxAndInsertListPageToPageListIfNecessary(idxInPageList, pageListHeadIdx);
        uint64_t numElementsToWriteToCurrentPage = std::min(
            remainingNumElementsToWrite, lists->numElementsPerPage - elementOffsetInListPage);
        StorageStructureUtils::updatePage(*(lists->getFileHandle()), lists->storageStructureID,
            listPageIdx, insertingNewPage, lists->bufferManager, *(lists->wal),
            [&inMemList, &elementOffsetInListPage, &numElementsToWriteToCurrentPage, &elementSize,
                &numUpdatedElements, this](uint8_t* frame) -> void {
                auto dataToFill = inMemList.getListData() + numUpdatedElements * elementSize;
                memcpy(frame + lists->getElemByteOffset(elementOffsetInListPage), dataToFill,
                    numElementsToWriteToCurrentPage * elementSize);
                if (inMemList.hasNullBuffer()) {
                    NullMask::copyNullMask(inMemList.getNullMask(), numUpdatedElements,
                        (uint64_t*)lists->getNullBufferInPage(frame), elementOffsetInListPage,
                        numElementsToWriteToCurrentPage);
                }
            });
        remainingNumElementsToWrite -= numElementsToWriteToCurrentPage;
        numUpdatedElements += numElementsToWriteToCurrentPage;
    }
}

} // namespace storage
} // namespace kuzu
