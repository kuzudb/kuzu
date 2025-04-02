#pragma once

#include <array>
#include <optional>
#include <set>

#include "common/types/types.h"
namespace kuzu::storage {

struct BlockEntry;

class FreeSpaceManager {
public:
    using free_list_t = std::vector<BlockEntry>;

    FreeSpaceManager();

    void addFreeChunk(BlockEntry entry);
    void addUncheckpointedFreeChunk(BlockEntry entry);

    // This also removes the chunk from the free space manager
    std::optional<BlockEntry> getFreeChunk(common::page_idx_t numPages);

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<FreeSpaceManager> deserialize(common::Deserializer& deSer);
    void checkpoint();

private:
    BlockEntry breakUpChunk(BlockEntry chunk, common::page_idx_t numRequiredPages);

    std::vector<free_list_t> freeLists;
    free_list_t uncheckpointedFreeChunks;
    common::page_idx_t numFreePages;
};

} // namespace kuzu::storage
