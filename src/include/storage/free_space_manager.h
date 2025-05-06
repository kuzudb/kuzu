/**
 * We would like to thank Mingkun Ni and Mayank Jasoria for doing the initial research and
 * prototyping for the FreeSpaceManager in their CS 848 course project:
 * https://github.com/ericpolo/kuzu_cs848
 */

#pragma once

#include <optional>
#include <set>

#include "common/types/types.h"
namespace kuzu::storage {

struct PageRange;
struct FreeEntryIterator;
class FileHandle;

class FreeSpaceManager {
public:
    static bool entryCmp(const PageRange& a, const PageRange& b);
    using sorted_free_list_t = std::set<PageRange, decltype(&entryCmp)>;
    using free_list_t = std::vector<PageRange>;

    FreeSpaceManager();

    void addFreePages(PageRange entry);
    std::optional<PageRange> popFreePages(common::page_idx_t numPages);

    // These pages are not reusable until the end of the next checkpoint
    void addUncheckpointedFreePages(PageRange entry);
    void rollbackCheckpoint();

    void serialize(common::Serializer& serializer) const;
    void deserialize(common::Deserializer& deSer);
    void finalizeCheckpoint(FileHandle* fileHandle);

    common::row_idx_t getNumEntries() const;
    std::vector<PageRange> getEntries(common::row_idx_t startOffset,
        common::row_idx_t endOffset) const;

private:
    PageRange splitPageRange(PageRange chunk, common::page_idx_t numRequiredPages);
    void mergePageRanges(free_list_t newInitialEntries, FileHandle* fileHandle);
    void handleLastPageRange(PageRange pageRange, FileHandle* fileHandle);
    void resetFreeLists();
    static common::idx_t getLevel(common::page_idx_t numPages);

    std::vector<sorted_free_list_t> freeLists;
    free_list_t uncheckpointedFreePageRanges;
    common::row_idx_t numEntries;
};

/**
 * Used for iterating over all entries in the FreeSpaceManager
 * Note that the iterator may become invalidated in the FSM is modified
 */
struct FreeEntryIterator {
    explicit FreeEntryIterator(const std::vector<FreeSpaceManager::sorted_free_list_t>& freeLists)
        : FreeEntryIterator(freeLists, 0) {}

    FreeEntryIterator(const std::vector<FreeSpaceManager::sorted_free_list_t>& freeLists,
        common::idx_t freeListIdx_)
        : freeLists(freeLists), freeListIdx(freeListIdx_) {
        advanceFreeListIdx();
    }

    void advance(common::row_idx_t numEntries);
    void operator++();
    PageRange operator*() const;
    bool done() const;

    void advanceFreeListIdx();

    const std::vector<FreeSpaceManager::sorted_free_list_t>& freeLists;
    common::idx_t freeListIdx;
    FreeSpaceManager::sorted_free_list_t::const_iterator freeListIt;
};

} // namespace kuzu::storage
