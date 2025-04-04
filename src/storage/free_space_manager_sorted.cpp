#include "common/utils.h"
#include "storage/block_manager.h"
#include "storage/free_space_manager.h"

namespace kuzu::storage {
static FreeSpaceManager::sorted_free_list_t& getFreeList(
    std::vector<FreeSpaceManager::sorted_free_list_t>& freeLists, common::idx_t level) {
    if (level >= freeLists.size()) {
        freeLists.resize(level + 1,
            FreeSpaceManager::sorted_free_list_t{&FreeSpaceManager::entryCmp});
    }
    return freeLists[level];
}

FreeSpaceManager::FreeSpaceManager() : freeLists{} {};

bool FreeSpaceManager::entryCmp(const BlockEntry& a, const BlockEntry& b) {
    return a.numPages == b.numPages ? a.startPageIdx < b.startPageIdx : a.numPages < b.numPages;
}

void FreeSpaceManager::addFreeChunk(BlockEntry entry) {
    const auto curLevel = common::countBits(entry.numPages) -
                          common::CountZeros<decltype(entry.numPages)>::Leading(entry.numPages) - 1;
    getFreeList(freeLists, curLevel).insert(entry);
}

void FreeSpaceManager::addUncheckpointedFreeChunk(BlockEntry entry) {
    uncheckpointedFreeChunks.push_back(entry);
}

// This also removes the chunk from the free space manager
std::optional<BlockEntry> FreeSpaceManager::getFreeChunk(common::page_idx_t numPages) {
    if (numPages > 0) {
        // 0b10 -> start at level 1
        // 0b11 -> start at level 1
        auto levelToSearch = common::countBits(numPages) -
                             common::CountZeros<decltype(numPages)>::Leading(numPages) - 1;
        for (; levelToSearch < freeLists.size(); ++levelToSearch) {
            auto& curList = freeLists[levelToSearch];
            auto entryIt = curList.lower_bound(BlockEntry{0, numPages});
            if (entryIt != curList.end()) {
                auto entry = *entryIt;
                curList.erase(entry);
                return breakUpChunk(entry, numPages);
            }
        }
    }
    return std::nullopt;
}

BlockEntry FreeSpaceManager::breakUpChunk(BlockEntry chunk, common::page_idx_t numRequiredPages) {
    KU_ASSERT(chunk.numPages >= numRequiredPages);
    BlockEntry ret{chunk.startPageIdx, numRequiredPages};
    if (numRequiredPages < chunk.numPages) {
        BlockEntry remainingEntry{chunk.startPageIdx + numRequiredPages,
            chunk.numPages - numRequiredPages};
        addFreeChunk(remainingEntry);
    }
    return ret;
}

void FreeSpaceManager::serialize(common::Serializer&) {}

std::unique_ptr<FreeSpaceManager> FreeSpaceManager::deserialize(common::Deserializer&) {
    return std::make_unique<FreeSpaceManager>();
}

void FreeSpaceManager::finalizeCheckpoint() {
    for (auto uncheckpointedEntry : uncheckpointedFreeChunks) {
        addFreeChunk(uncheckpointedEntry);
    }
    uncheckpointedFreeChunks.clear();

    combineAdjacentChunks();
}

void FreeSpaceManager::combineAdjacentChunks() {
    std::vector<BlockEntry> allEntries;
    for (const auto& freeList : freeLists) {
        allEntries.insert(allEntries.end(), freeList.begin(), freeList.end());
    }
    if (allEntries.empty()) {
        return;
    }

    freeLists.clear();
    std::sort(allEntries.begin(), allEntries.end(), [](const auto& entryA, const auto& entryB) {
        return entryA.startPageIdx < entryB.startPageIdx;
    });

    BlockEntry prevEntry = allEntries[0];
    for (common::row_idx_t i = 1; i < allEntries.size(); ++i) {
        const auto& entry = allEntries[i];
        if (prevEntry.startPageIdx + prevEntry.numPages == entry.startPageIdx) {
            prevEntry.numPages += entry.numPages;
        } else {
            addFreeChunk(prevEntry);
            prevEntry = entry;
        }
    }
    addFreeChunk(prevEntry);
}

common::row_idx_t FreeSpaceManager::getNumEntries() const {
    common::row_idx_t ret = 0;
    for (const auto& freeList : freeLists) {
        ret += freeList.size();
    }
    return ret;
}

static std::pair<common::idx_t, common::row_idx_t> getStartFreeEntry(common::row_idx_t startOffset,
    const std::vector<FreeSpaceManager::sorted_free_list_t>& freeLists) {
    size_t freeListIdx = 0;
    common::row_idx_t curOffset = 0;
    while (startOffset - curOffset >= freeLists[freeListIdx].size()) {
        KU_ASSERT(freeListIdx < freeLists.size());
        curOffset += freeLists[freeListIdx].size();
        ++freeListIdx;
    }
    return {freeListIdx, startOffset - curOffset};
}

std::vector<BlockEntry> FreeSpaceManager::getEntries(common::row_idx_t startOffset,
    common::row_idx_t endOffset) const {
    KU_ASSERT(endOffset >= startOffset);
    std::vector<BlockEntry> ret;
    auto [freeListIdx, idxInList] = getStartFreeEntry(startOffset, freeLists);
    for (; freeListIdx < freeLists.size(); ++freeListIdx) {
        auto it = freeLists[freeListIdx].begin();
        // TODO(Royi) optimize this
        std::advance(it, idxInList);
        for (; it != freeLists[freeListIdx].end(); ++it) {
            if (ret.size() >= endOffset - startOffset) {
                return ret;
            }
            ret.push_back(*it);
        }
        idxInList = 0;
    }
    return ret;
}

} // namespace kuzu::storage
