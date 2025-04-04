#pragma once

#include <optional>
#include <set>

#include "common/types/types.h"
namespace kuzu::storage {

struct BlockEntry;

class FreeSpaceManager {
public:
    static bool entryCmp(const BlockEntry& a, const BlockEntry& b);

    using free_list_t = std::vector<BlockEntry>;
    using sorted_free_list_t = std::set<BlockEntry, decltype(&entryCmp)>;

    FreeSpaceManager();

    void addFreeChunk(BlockEntry entry);
    void addUncheckpointedFreeChunk(BlockEntry entry);

    // This also removes the chunk from the free space manager
    std::optional<BlockEntry> getFreeChunk(common::page_idx_t numPages);

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<FreeSpaceManager> deserialize(common::Deserializer& deSer);
    void finalizeCheckpoint();

    common::row_idx_t getNumEntries() const;
    std::vector<BlockEntry> getEntries(common::row_idx_t startOffset,
        common::row_idx_t endOffset) const;

private:
    BlockEntry breakUpChunk(BlockEntry chunk, common::page_idx_t numRequiredPages);
    void combineAdjacentChunks();

    std::vector<sorted_free_list_t> freeLists;
    // std::vector<free_list_t> freeLists;
    free_list_t uncheckpointedFreeChunks;
};

} // namespace kuzu::storage
