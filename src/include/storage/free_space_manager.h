#pragma once

#include <optional>
#include <set>

#include "common/types/types.h"
namespace kuzu::storage {

struct PageChunkEntry;

class FreeSpaceManager {
public:
    static bool entryCmp(const PageChunkEntry& a, const PageChunkEntry& b);

    using free_list_t = std::vector<PageChunkEntry>;
    using sorted_free_list_t = std::set<PageChunkEntry, decltype(&entryCmp)>;

    FreeSpaceManager();

    void addFreeChunk(PageChunkEntry entry);
    void addUncheckpointedFreeChunk(PageChunkEntry entry);

    // This also removes the chunk from the free space manager
    std::optional<PageChunkEntry> getFreeChunk(common::page_idx_t numPages);

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<FreeSpaceManager> deserialize(common::Deserializer& deSer);
    void finalizeCheckpoint();

    common::row_idx_t getNumEntries() const;
    std::vector<PageChunkEntry> getEntries(common::row_idx_t startOffset,
        common::row_idx_t endOffset) const;

private:
    PageChunkEntry breakUpChunk(PageChunkEntry chunk, common::page_idx_t numRequiredPages);
    void combineAdjacentChunks();

    std::vector<sorted_free_list_t> freeLists;
    // std::vector<free_list_t> freeLists;
    free_list_t uncheckpointedFreeChunks;
};

} // namespace kuzu::storage
