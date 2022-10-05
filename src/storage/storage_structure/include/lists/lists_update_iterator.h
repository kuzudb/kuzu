#pragma once

#include "unstructured_property_lists.h"

namespace graphflow {
namespace storage {

/*
 * This class implements the CSR reconstruction algorithm to update UnstructuredPropertyLists
 * transactionally. It is designed to be used as follows through calls to updateLists:
 *     UnstructuredPropertyListsUpdateIterator updateItr(this);
 *     for (auto updatedChunkItr = localUpdatedLists.updatedChunks.begin();
 *         updatedChunkItr != localUpdatedLists.updatedChunks.end(); ++updatedChunkItr) {
 *         for (auto updatedNodeOffsetItr = updatedChunkItr->second->begin();
 *              updatedNodeOffsetItr != updatedChunkItr->second->end(); updatedNodeOffsetItr++) {
 *              updateItr.updateList(updatedNodeOffsetItr->first,
 *                  updatedNodeOffsetItr->second->data.get() (the new list in uint8_t*),
 *                  updatedNodeOffsetItr->second->size (size of the new list));
 *         }
 *     }
 * Two important requirements for correctly using this class are: (i) that the
 * calls to updateList need to happen in ascending order of nodeOffsets; and (ii) there should not
 * be multiple calls to writeListToListPages for the same nodeOffset.
 *
 * The CSR re-construction algorithm is currently very basic and slides each small list to the right
 * or left when updates to a chunk arrives (large lists don't need sliding because recall that they
 * are not part of the CSR and they get their own list of pages), simply by reading them in their
 * entirety and writing them into their new places. Small lists that become large, get their own
 * page lists. Large lists do not become small if they become small. Large lists are also written in
 * their entirety.
 *
 * We clarify the following abbreviations and conventions in the variables used in these files:
 * <ul>
 *   <li> idxInPageList: refers to an index in the page list of a chunk or large list and is
 *        consecutive from 0 to # pages in the page list.
 *   <li> listPageIdx: physical pageIdx in the .lists file.
 * </ul>
 */

class ListsUpdateIterator {
public:
    ListsUpdateIterator(Lists* lists)
        : lists{lists}, curChunkIdx{UINT64_MAX}, curUnprocessedNodeOffset{UINT64_MAX},
          curCSROffset{UINT64_MAX}, finishCalled{false},
          tmpDataChunkState{make_shared<DataChunkState>(DEFAULT_VECTOR_CAPACITY)},
          // Note: We use DataTypeID:BOOL below because we need ValueVector into which
          // we can write 1 byte-sized elements. valueVectorToScanSmallLists is used when sliding
          // small lists left and right during CSR construction as a buffer to
          // copy the lists from their old places in pages and then writing them to new location in
          // pages. To read the pages, we use Lists::readSmallList() function, which can internally
          // memcpy lists from pages into the buffer of ValueVector. This function reads the size of
          // the list from ListInfo (which is based on the header of a node) and reads
          // size*elementSize many bytes into the ValueVector. Since UnstructuredPropertyLists take
          // elementSize as 1, we need a 1 byte-sized data type. Note that the functions in
          // UnstructuredPropertyLists to read properties do not read
          valueVectorToScanSmallLists{make_shared<ValueVector>(DataTypeID::BOOL)} {
        valueVectorToScanSmallLists->setState(tmpDataChunkState);
    }

    ~ListsUpdateIterator() { assert(finishCalled); }

    virtual void updateList(node_offset_t nodeOffset, InMemList& inMemList);

    void doneUpdating();

protected:
    void seekToBeginningOfChunkIdx(uint64_t chunkIdx);

    virtual void slideListsIfNecessary(uint64_t endNodeOffsetInclusive);

    void seekToNodeOffsetAndSlideListsIfNecessary(node_offset_t nodeOffsetToSeekTo);

    void writeListToListPages(InMemList& inMemList, page_idx_t pageListHeadIdx, bool isSmallList);

    void updateLargeList(list_header_t oldHeader, InMemList& inMemList);

    virtual void updateSmallListAndCurCSROffset(list_header_t oldHeader, InMemList& inMemList);

    pair<page_idx_t, bool> findListPageIdxAndInsertListPageToPageListIfNecessary(
        page_idx_t idxInPageList, uint32_t pageListHeadIdx);
    // beginIdxOfCurrentPageGroup == UINT32_MAX indicates that this is the first pageGroup of the
    // current chunk or large list, so the function sets the chunkToHeadIdxMap of the chunk or
    // largeListIdxToPageListHeadIdxMap for the given largeListID.
    // largeListIdx == UINT32_MAX indicates that this page group is inserted as part of small list
    // update, so if beginIdxOfCurrentPageGroup == UINT32_MAX, the chunkToPageListHeadIdxMap will be
    // updated; otherwise largeListIdxToPageListHeadIdxMap will be updated.
    page_idx_t insertNewPageGroupAndSetHeadIdxMap(
        uint32_t beginIdxOfCurrentPageGroup, uint32_t largeListIdx = UINT32_MAX);

    virtual inline bool requireNullMask() const { return false; }

protected:
    Lists* lists;
    uint64_t curChunkIdx;
    uint64_t curUnprocessedNodeOffset;
    uint64_t curCSROffset;
    bool finishCalled;
    shared_ptr<DataChunkState> tmpDataChunkState;
    shared_ptr<ValueVector> valueVectorToScanSmallLists;
};

class RelPropertyListUpdateIterator : public ListsUpdateIterator {
public:
    RelPropertyListUpdateIterator(Lists* lists) : ListsUpdateIterator(lists) {}

    inline bool requireNullMask() const override { return true; }
};

} // namespace storage
} // namespace graphflow
