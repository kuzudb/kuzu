#include "storage/free_space_manager.h"

#include "storage/block_manager.h"

namespace kuzu::storage {
FreeSpaceManager::FreeSpaceManager() : freeLists{}, numFreePages(0) {};

void FreeSpaceManager::addFreeChunk(BlockEntry entry) {}

// This also removes the chunk from the free space manager
std::optional<BlockEntry> FreeSpaceManager::getFreeChunk(common::page_idx_t numPages) {}

void FreeSpaceManager::serialize(common::Serializer&) {}

std::unique_ptr<FreeSpaceManager> FreeSpaceManager::deserialize(common::Deserializer&) {
    return std::make_unique<FreeSpaceManager>();
}
} // namespace kuzu::storage
