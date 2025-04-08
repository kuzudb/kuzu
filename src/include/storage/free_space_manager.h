#pragma once

#include <optional>
#include <set>

#include "common/types/types.h"
namespace kuzu::storage {

struct PageChunkEntry;
struct FreeEntryIterator;

class FreeSpaceManager {
public:
    static bool entryCmp(const PageChunkEntry& a, const PageChunkEntry& b);
    using sorted_free_list_t = std::set<PageChunkEntry, decltype(&entryCmp)>;

    FreeSpaceManager();

    void addFreeChunk(PageChunkEntry entry);
    std::optional<PageChunkEntry> popFreeChunk(common::page_idx_t numPages);

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<FreeSpaceManager> deserialize(common::Deserializer& deSer);
    void finalizeCheckpoint();

    common::row_idx_t getNumEntries() const;
    std::vector<PageChunkEntry> getEntries(common::row_idx_t startOffset,
        common::row_idx_t endOffset) const;

private:
    PageChunkEntry breakUpChunk(PageChunkEntry chunk, common::page_idx_t numRequiredPages);
    void combineAdjacentChunks();
    void reset();
    static common::idx_t getLevel(common::page_idx_t numPages);

    std::vector<sorted_free_list_t> freeLists;
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
    PageChunkEntry operator*() const;
    bool done() const;

    void advanceFreeListIdx();

    const std::vector<FreeSpaceManager::sorted_free_list_t>& freeLists;
    common::idx_t freeListIdx;
    FreeSpaceManager::sorted_free_list_t::const_iterator freeListIt;
};

} // namespace kuzu::storage
