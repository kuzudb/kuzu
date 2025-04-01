#include "storage/free_space_manager.h"

#include "common/utils.h"
#include "storage/block_manager.h"

namespace kuzu::storage {
FreeSpaceManager::FreeSpaceManager() : freeLists{}, numFreePages(0) {};

void FreeSpaceManager::addFreeChunk(BlockEntry entry) {
    // KU_ASSERT(std::has_single_bit(entry.numPages));
    auto numPages = entry.numPages;
    auto startPageIdx = entry.startPageIdx;
    while (numPages > 0) {
        const auto curLevel = common::CountZeros<decltype(numPages)>::Trailing(numPages);
        const auto pagesInLevel = static_cast<decltype(numPages)>(1) << curLevel;
        freeLists[curLevel].push_back(BlockEntry{startPageIdx, pagesInLevel, entry.blockManager});
        numPages -= pagesInLevel;
        startPageIdx += pagesInLevel;
    }
    numFreePages += entry.numPages;
}

// This also removes the chunk from the free space manager
std::optional<BlockEntry> FreeSpaceManager::getFreeChunk(common::page_idx_t numPages) {
    if (numPages > 0) {
        auto levelToSearch = common::countBits(numPages) -
                             common::CountZeros<decltype(numPages)>::Leading(numPages - 1);
        for (; levelToSearch < freeLists.size(); ++levelToSearch) {
            auto& curList = freeLists[levelToSearch];
            if (!curList.empty()) {
                auto entry = curList.back();
                curList.pop_back();
                numFreePages -= numPages;
                return breakUpChunk(entry, numPages);
            }
        }
    }
    return std::nullopt;
}

BlockEntry FreeSpaceManager::breakUpChunk(BlockEntry chunk, common::page_idx_t numRequiredPages) {
    KU_ASSERT(chunk.numPages >= numRequiredPages);
    BlockEntry ret{chunk.startPageIdx, numRequiredPages, chunk.blockManager};

    common::page_idx_t remainingStartPage = chunk.startPageIdx + numRequiredPages;
    common::page_idx_t numRemainingPages = chunk.numPages - numRequiredPages;
    while (numRemainingPages > 0) {
        const common::page_idx_t curChunkLevel =
            common::CountZeros<decltype(numRemainingPages)>::Trailing(numRemainingPages);
        const common::page_idx_t numPagesInLevel = static_cast<common::page_idx_t>(1)
                                                   << curChunkLevel;
        freeLists[curChunkLevel].push_back(
            BlockEntry{remainingStartPage, numPagesInLevel, chunk.blockManager});
        remainingStartPage += numPagesInLevel;
        numRemainingPages -= numPagesInLevel;
    }
    return ret;
}

void FreeSpaceManager::serialize(common::Serializer&) {}

std::unique_ptr<FreeSpaceManager> FreeSpaceManager::deserialize(common::Deserializer&) {
    return std::make_unique<FreeSpaceManager>();
}
} // namespace kuzu::storage
