#include "src/storage/storage_structure/include/lists/lists_update_iterator.h"

#include "src/storage/storage_structure/include/storage_structure_utils.h"

namespace graphflow {
namespace storage {

void ListsUpdateIterator::updateList(node_offset_t nodeOffset, InMemList& inMemList) {
    seekToNodeOffsetAndSlideListsIfNecessary(nodeOffset);
    // We first check if the updated list is a newly inserted list.
    auto headers = lists->getHeaders();
    uint64_t numNodes = headers->headersDiskArray->getNumElements(TransactionType::WRITE);
    assert(nodeOffset <= numNodes);
    // Updates can be made to:
    // (i) Existing node offset (i.e.,nodeOffset < numNodes); (ii) A new list for a new node that
    // has been inserted (so nodeOffset should be numNodes in this case).
    uint64_t oldHeader;
    if (nodeOffset == numNodes) {
        assert(lists->storageStructureIDAndFName.storageStructureID.listFileID.listType ==
               ListType::UNSTRUCTURED_NODE_PROPERTY_LISTS);
        oldHeader = ListHeaders::getSmallListHeader(0, 0);
        headers->headersDiskArray->pushBack(0);
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

void ListsUpdateIterator::doneUpdating() {
    if (curChunkIdx != UINT64_MAX) {
        // If we are done updating, we should seek to the end of the curChunkIdx. However
        // curChunkIdx may be the last chunk and the maximum node offset may be smaller than the
        // maximum nodeOffset that would be in curChunkIdx if the curChunkIdx was full (which is
        // what is returned by getChunkIdxEndNodeOffsetInclusive(curChunkIdx)). So we take the
        // minimum of max Note also that we are trying to seek to the nodeOffset to the immediate
        // right of the last node we want to slide. We do not have a "+ 1" for
        // headersDiskArray->getNumElements, because that is exactly + 1 the last nodeOffset we want
        // to slide. We have a + 1 for getChunkIdxEndNodeOffsetInclusive(curChunkIdx) because we
        // want to slide the node with offset getChunkIdxEndNodeOffsetInclusive(curChunkIdx).
        auto endNodeOffsetToSeekTo =
            min(lists->getHeaders()->headersDiskArray->getNumElements(TransactionType::WRITE),
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
    for (node_offset_t nodeOffsetToSlide = curUnprocessedNodeOffset;
         nodeOffsetToSlide <= endNodeOffsetInclusive; ++nodeOffsetToSlide) {
        list_header_t oldHeader = lists->getHeaders()->headersDiskArray->get(
            nodeOffsetToSlide, TransactionType::READ_ONLY);
        // Because large lists get their own set of pages and are not part of the CSR maintained
        // in the current chunk's page list, we do not need to slide them. Only small lists need to
        // be slided if necessary. The necessity condition is if the old header is not equal to
        // the new header that would be formed by the current CSR offset.
        if (!ListHeaders::isALargeList(oldHeader)) {
            // Note that we do not need to differentiate whether this function is running for
            // AdjLists-UnstructuredPropertyLists (which need to compute newHeaders and actually
            // update them) vs RelPropertyLists (which do not update headers and can always read new
            // headers from the updated version of the header). This is because AdjList and
            // RelPropertyLists are parallel and if a list remained small then, we would iteratively
            // always compute the same new csrOffsets for each small list header.
            auto [listLen, csrOffset] = ListHeaders::getSmallListLenAndCSROffset(oldHeader);
            list_header_t newHeader = ListHeaders::getSmallListHeader(curCSROffset, listLen);
            if (newHeader != oldHeader) {
                InMemList inMemList{listLen, lists->elementSize, lists->mayContainNulls()};
                CursorAndMapper cursorAndMapper;
                cursorAndMapper.reset(lists->getListsMetadata(), lists->numElementsPerPage,
                    lists->getHeaders()->getHeader(nodeOffsetToSlide), nodeOffsetToSlide);
                lists->fillInMemListsFromPersistentStore(cursorAndMapper,
                    lists->getNumElementsFromListHeader(nodeOffsetToSlide), inMemList);
                updateSmallListAndCurCSROffset(oldHeader, inMemList);
            } else {
                curCSROffset += listLen;
            }
        }
        curUnprocessedNodeOffset++;
    }
}

void ListsUpdateIterator::seekToNodeOffsetAndSlideListsIfNecessary(
    node_offset_t nodeOffsetToSeekTo) {
    auto chunkIdxOfNode = StorageUtils::getListChunkIdx(nodeOffsetToSeekTo);
    if (curChunkIdx == UINT64_MAX) {
        seekToBeginningOfChunkIdx(chunkIdxOfNode);
    } else if (curChunkIdx != chunkIdxOfNode) {
        // If the next node offset whose list will be updated is in another chunk, and if
        // curChunkIdx was an actual chunkIdx (i.e., it was not null=UINT64_MAX), we first slide (if
        // necessary) any lists from the curUnprocessedNodeOffset (inclusive) to the last node
        // offset in the current chunkIdx (inclusive)
        slideListsIfNecessary(StorageUtils::getChunkIdxEndNodeOffsetInclusive(curChunkIdx));
        seekToBeginningOfChunkIdx(chunkIdxOfNode);
    }
    // At this point we are guaranteed to have seeked to a node offset that is the same chunkIdx as
    // the nodeOffsetToSeekTo. So we first slide (if necessary) all lists until nodeOffsetToSeekTo -
    // 1 (inclusive). This will ensure that the curUnprocessedNodeOffset is nodeOffsetToSeekTo,
    // so the writeListToListPages function can update the list of nodeOffsetToSeekTo.
    if (nodeOffsetToSeekTo > 0) {
        slideListsIfNecessary(nodeOffsetToSeekTo - 1);
    }
}

void ListsUpdateIterator::writeListToListPages(
    InMemList& inMemList, page_idx_t pageListHeadIdx, bool isSmallList) {
    uint64_t idxInPageList;
    uint64_t elementOffsetInListPage;
    uint64_t elementSize = lists->elementSize;
    if (isSmallList) {
        pair<uint64_t, uint64_t> idInPageGroupAndOffsetInListPage =
            StorageUtils::getQuotientRemainder(curCSROffset, lists->numElementsPerPage);
        idxInPageList = idInPageGroupAndOffsetInListPage.first;
        elementOffsetInListPage = idInPageGroupAndOffsetInListPage.second;
    } else {
        // For large lists, the firstIdxInPageGroup is always 0.
        idxInPageList = 0;
        elementOffsetInListPage = 0;
    }

    bool firstIteration = true;
    auto remainingNumElementsToWrite = inMemList.numElements;
    uint64_t numUpdatedElements = 0;
    auto numElementsPerPage = lists->numElementsPerPage;
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
        uint64_t numElementsToWriteToCurrentPage =
            min(remainingNumElementsToWrite, lists->numElementsPerPage - elementOffsetInListPage);
        StorageStructureUtils::updatePage(*(lists->getFileHandle()), listPageIdx, insertingNewPage,
            lists->bufferManager, *(lists->wal),
            [&inMemList, &elementOffsetInListPage, &numElementsToWriteToCurrentPage, &elementSize,
                &numElementsPerPage, &numUpdatedElements](uint8_t* frame) -> void {
                auto dataToFill = inMemList.getListData() + numUpdatedElements * elementSize;
                memcpy(frame + elementOffsetInListPage * elementSize, dataToFill,
                    numElementsToWriteToCurrentPage * elementSize);
                if (inMemList.hasNullBuffer()) {
                    NullMask::copyNullMask(inMemList.getNullMask(), numUpdatedElements,
                        (uint64_t*)(frame + numElementsPerPage * elementSize),
                        elementOffsetInListPage, numElementsToWriteToCurrentPage);
                }
            });
        remainingNumElementsToWrite -= numElementsToWriteToCurrentPage;
        numUpdatedElements += numElementsToWriteToCurrentPage;
    }
}

void ListsUpdateIterator::updateLargeList(list_header_t oldHeader, InMemList& inMemList) {
    page_idx_t pageListHeadIdx;
    uint32_t largeListIdx;
    if (ListHeaders::isALargeList(oldHeader)) {
        largeListIdx = ListHeaders::getLargeListIdx(oldHeader);
        // Note that we can actually directly read from the original
        // largeListIdxToPageListHeadIdxMap here since this listUpdateIterator should be called only
        // once when prepareToCommit on lists is called, and we should be updating each node's list
        // at most once. Therefore the read and write version of the values for pageListHeadIdx for
        // the curUnprocessedNodeOffset should be the same.
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
        // adjOrUnstructuredPropertyListsUpdateIterator because relPropertyList share the same
        // header with adjList. The below updateLargeListHeaderIfNecessary(...) function will only
        // update the header if the executing class is adjOrUnstructuredPropertyListsUpdateIterator.
        updateLargeListHeaderIfNecessary(largeListIdx);
        // 3) Then we set a NULL pageListHeadIdx and also NULL for numElements of the new large
        // list. The call to insertNewPageGroupAndSetHeadIdxMap will update the pageListHeadIdx. And
        // after the else branch we will insert the numElements.
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
    writeListToListPages(inMemList, pageListHeadIdx, false /* isSmallList */);
}

void ListsUpdateIterator::updateSmallListAndCurCSROffset(
    list_header_t oldHeader, InMemList& inMemList) {
    // Note that we do not need to differentiate whether this function is running for
    // AdjLists-UnstructuredPropertyLists (which need to compute newHeaders and actually
    // update them) vs RelPropertyLists (which do not update headers and can always read new
    // headers from the updated version of the header). This is because AdjList and
    // RelPropertyLists are parallel and if a list remained small then, we would iteratively
    // always compute the same new csrOffsets for each small list header.
    list_header_t newHeader = ListHeaders::getSmallListHeader(curCSROffset, inMemList.numElements);
    // 1: Only the adjOrUnstructuredPropertyListsUpdateIterator need to update
    // the header, so the below updateSmallListHeaderIfNecessary(...) will only update the header
    // if the executing class is adjOrUnstructuredPropertyListsUpdateIterator.
    updateSmallListHeaderIfNecessary(oldHeader, newHeader);
    // If the newList is empty we can return. Changing the header is enough.
    if (inMemList.numElements <= 0) {
        return;
    }
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

    // If the chunk head idx is null, insert an initial page group for the chunk.
    if (pageListHeadIdx == StorageStructureUtils::NULL_CHUNK_OR_LARGE_LIST_HEAD_IDX) {
        // Note that for small lists we don't pass the second argument (the largeListIdx) to
        // insertNewPageGroupAndSetHeadIdxMap, which is passed automatically as null=UINT32_MAX.
        pageListHeadIdx = insertNewPageGroupAndSetHeadIdxMap(
            UINT32_MAX /* no curIdxInPageList; update chunkToPageListHeadIdxMap */);
    }

    // 3: Write the newList to the list pages.
    writeListToListPages(inMemList, pageListHeadIdx, true /* is small list */);
    // 4: Update the curCSROffset
    curCSROffset += inMemList.numElements;
}

pair<page_idx_t, bool> ListsUpdateIterator::findListPageIdxAndInsertListPageToPageListIfNecessary(
    page_idx_t idxInPageList, uint32_t pageListHeadIdx) {
    auto pageLists = lists->getListsMetadata().pageLists.get();
    uint32_t curIdxInPageList = pageListHeadIdx;
    while (ListsMetadataConfig::PAGE_LIST_GROUP_SIZE <= idxInPageList) {
        auto nextIdxInPageList =
            pageLists->get(curIdxInPageList + ListsMetadataConfig::PAGE_LIST_GROUP_SIZE, WRITE);
        if (nextIdxInPageList == StorageStructureUtils::NULL_PAGE_IDX) {
            // If idxInPageList >= ListsMetadataConfig::PAGE_LIST_GROUP_SIZE but the
            // current page group does not have a new page group, it must be the case that
            // the idxInPageList is the first page in the next page group. This is because we
            // are consecutively updating the lists. suppose next list needs to be put in
            // page x in the page list, i.e., the idxInPageList for the next list is x, but
            // currently, the page group to hold x does not exist, then it has to be the case that
            // there are page groups (and list pages) for up to page x-1, and not for x, x+1, and
            // x+2 (assuming PAGE_GROUP_SIZE is 3). For example x can be 3, and we may have only 1
            // page group. But x cannot be 4 and we ran out of page groups because the previous
            // list we stored would have created the page group for 3, 4, and 5 (note the first page
            // group would store x 0, 1, and 2).
            assert(idxInPageList == ListsMetadataConfig::PAGE_LIST_GROUP_SIZE);
            curIdxInPageList = insertNewPageGroupAndSetHeadIdxMap(curIdxInPageList);
        } else {
            curIdxInPageList = nextIdxInPageList;
        }
        idxInPageList -= ListsMetadataConfig::PAGE_LIST_GROUP_SIZE;
    }
    curIdxInPageList += idxInPageList;
    page_idx_t listPageIdx = pageLists->get(curIdxInPageList, WRITE);
    bool isInsertingNewListPage = false;
    if (listPageIdx == StorageStructureUtils::NULL_PAGE_IDX) {
        isInsertingNewListPage = true;
        listPageIdx = lists->getFileHandle()->addNewPage();
        pageLists->update(curIdxInPageList, listPageIdx);
    }
    return make_pair(listPageIdx, isInsertingNewListPage);
}

page_idx_t ListsUpdateIterator::insertNewPageGroupAndSetHeadIdxMap(
    uint32_t beginIdxOfCurrentPageGroup, uint32_t largeListIdx) {
    auto pageLists = lists->getListsMetadata().pageLists.get();
    auto beginIdxOfNextPageGroup = pageLists->getNumElements(TransactionType::WRITE);
    // If the beginIdxOfCurrentPageGroup is null (identified as UINT32_MAX) then we update the list
    // header.
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
        pageLists->update(beginIdxOfCurrentPageGroup + ListsMetadataConfig::PAGE_LIST_GROUP_SIZE,
            beginIdxOfNextPageGroup);
    }
    for (int i = 0; i < ListsMetadataConfig::PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE; ++i) {
        pageLists->pushBack(StorageStructureUtils::NULL_PAGE_IDX);
    }
    return beginIdxOfNextPageGroup;
}

} // namespace storage
} // namespace graphflow
