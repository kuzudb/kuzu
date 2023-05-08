#pragma once

#include "lists.h"

namespace kuzu {
namespace storage {

/*
 * This class implements the CSR reconstruction algorithm to update lists transactionally.
 * It is designed to be used as follows through calls to updateLists:
 *     auto updateItr = ListsUpdateIteratorFactory::getListsUpdateIterator(this);
 *     for (auto updatedChunkItr = listsUpdatesStore.updatedChunks.begin();
 *         updatedChunkItr != listsUpdatesStore.updatedChunks.end(); ++updatedChunkItr) {
 *         for (auto updatedNodeOffsetItr = updatedChunkItr->second->begin();
 *              updatedNodeOffsetItr != updatedChunkItr->second->end(); updatedNodeOffsetItr++) {
 *              updateItr.updateList(nodeOffset, inMemList);
 *         }
 *     }
 * Two important requirements for correctly using this class are: (i) that the
 * calls to updateList need to happen in ascending order of nodeOffsets; and (ii) there should not
 * be multiple calls to writeInMemListToListPages for the same nodeOffset.
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
    explicit ListsUpdateIterator(Lists* lists)
        : lists{lists}, curChunkIdx{UINT64_MAX}, curUnprocessedNodeOffset{UINT64_MAX},
          curCSROffset{UINT32_MAX}, finishCalled{false} {}

    virtual ~ListsUpdateIterator() { assert(finishCalled); }

    void updateList(common::offset_t nodeOffset, InMemList& inMemList);

    void doneUpdating();

protected:
    virtual inline void updateListHeaderIfNecessary(
        csr_offset_t oldCSROffset, csr_offset_t newCSROffset) = 0;

    void seekToBeginningOfChunkIdx(uint64_t chunkIdx);

    void slideListsIfNecessary(uint64_t endNodeOffsetInclusive);

    bool seekToNodeOffsetAndSlideListsIfNecessary(common::offset_t nodeOffsetToSeekTo);

    void writeInMemListToListPages(InMemList& inMemList, common::page_idx_t pageListHeadIdx);

    void updateListAndCurCSROffset(csr_offset_t oldCSROffset, InMemList& inMemList);

    std::pair<common::page_idx_t, bool> findListPageIdxAndInsertListPageToPageListIfNecessary(
        common::page_idx_t idxInPageList, uint32_t pageListHeadIdx);

private:
    // beginIdxOfCurrentPageGroup == UINT32_MAX indicates that this is the first pageGroup of the
    // current chunk or large list, so the function sets the chunkToHeadIdxMap of the chunk or
    // largeListIdxToPageListHeadIdxMap for the given largeListID.
    // largeListIdx == UINT32_MAX indicates that this page group is inserted as part of small list
    // update, so if beginIdxOfCurrentPageGroup == UINT32_MAX, the chunkToPageListHeadIdxMap will be
    // updated; otherwise largeListIdxToPageListHeadIdxMap will be updated.
    common::page_idx_t insertNewPageGroupAndSetHeadIdxMap(uint32_t beginIdxOfCurrentPageGroup);

    void writeAtOffset(InMemList& inMemList, common::page_idx_t pageListHeadIdx,
        uint64_t idxInPageList, uint64_t elementOffsetInListPage);

protected:
    Lists* lists;
    uint64_t curChunkIdx;
    uint64_t curUnprocessedNodeOffset;
    csr_offset_t curCSROffset;
    bool finishCalled;
};

class AdjListsUpdateIterator : public ListsUpdateIterator {
public:
    explicit AdjListsUpdateIterator(Lists* lists) : ListsUpdateIterator{lists} {}

private:
    inline void updateListHeaderIfNecessary(
        csr_offset_t oldCSROffset, csr_offset_t newCSROffset) override {
        if (newCSROffset != oldCSROffset) {
            lists->getHeaders()->update(curUnprocessedNodeOffset, newCSROffset);
        }
    }
};

class RelPropertyListsUpdateIterator : public ListsUpdateIterator {
public:
    explicit RelPropertyListsUpdateIterator(Lists* lists) : ListsUpdateIterator{lists} {}

private:
    // Only adjListUpdateIterator need to update list header.
    inline void updateListHeaderIfNecessary(
        csr_offset_t oldCSROffset, csr_offset_t newCSROffset) override {}
};

class ListsUpdateIteratorFactory {
public:
    static std::unique_ptr<ListsUpdateIterator> getListsUpdateIterator(Lists* lists) {
        switch (lists->storageStructureIDAndFName.storageStructureID.listFileID.listType) {
        case ListType::ADJ_LISTS:
            return std::make_unique<AdjListsUpdateIterator>(lists);
        case ListType::REL_PROPERTY_LISTS:
            return std::make_unique<RelPropertyListsUpdateIterator>(lists);
        default:
            assert(false);
        }
    }
};

} // namespace storage
} // namespace kuzu
