#pragma once

#include <array>
#include <optional>
#include <set>

#include "common/types/types.h"
namespace kuzu::storage {

struct BlockEntry;

enum class FreeEntryLevel : uint8_t {
    FREE_ENTRY_LEVEL_1 = 0,
    FREE_ENTRY_LEVEL_2,
    FREE_ENTRY_LEVEL_4,
    FREE_ENTRY_LEVEL_8,
    FREE_ENTRY_LEVEL_16,
    FREE_ENTRY_LEVEL_32,
    FREE_ENTRY_LEVEL_64,
    FREE_ENTRY_LEVEL_128,
    FREE_ENTRY_LEVEL_256,
    FREE_ENTRY_LEVEL_MAX,
};
static constexpr uint8_t numFreeEntryLevels =
    static_cast<uint8_t>(FreeEntryLevel::FREE_ENTRY_LEVEL_MAX);

class FreeSpaceManager {
public:
    FreeSpaceManager();

    void addFreeChunk(BlockEntry entry);
    // void addUncheckpointedFreeChunk(BlockEntry entry);

    // This also removes the chunk from the free space manager
    std::optional<BlockEntry> getFreeChunk(common::page_idx_t numPages);

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<FreeSpaceManager> deserialize(common::Deserializer& deSer);

private:
    std::array<std::set<BlockEntry>, numFreeEntryLevels> freeLists;
    common::page_idx_t numFreePages;
};

} // namespace kuzu::storage
