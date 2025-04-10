#include "storage/free_space_manager.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/utils.h"
#include "storage/page_chunk_entry.h"

namespace kuzu::storage {
static FreeSpaceManager::sorted_free_list_t& getFreeList(
    std::vector<FreeSpaceManager::sorted_free_list_t>& freeLists, common::idx_t level) {
    if (level >= freeLists.size()) {
        freeLists.resize(level + 1,
            FreeSpaceManager::sorted_free_list_t{&FreeSpaceManager::entryCmp});
    }
    return freeLists[level];
}

FreeSpaceManager::FreeSpaceManager() : freeLists{}, numEntries(0){};

common::idx_t FreeSpaceManager::getLevel(common::page_idx_t numPages) {
    // level is exponent of largest power of 2 that is <= numPages
    // e.g. 2 -> level 1, 5 -> level 2
    return common::countBits(numPages) - common::CountZeros<common::page_idx_t>::Leading(numPages) -
           1;
}

bool FreeSpaceManager::entryCmp(const PageRange& a, const PageRange& b) {
    return a.numPages == b.numPages ? a.startPageIdx < b.startPageIdx : a.numPages < b.numPages;
}

void FreeSpaceManager::addFreePages(PageRange entry) {
    getFreeList(freeLists, getLevel(entry.numPages)).insert(entry);
    ++numEntries;
}

void FreeSpaceManager::addUncheckpointedFreePages(PageRange entry) {
    uncheckpointedFreePageRanges.push_back(entry);
}

// This also removes the chunk from the free space manager
std::optional<PageRange> FreeSpaceManager::popFreePages(common::page_idx_t numPages) {
    if (numPages > 0) {
        auto levelToSearch = getLevel(numPages);
        for (; levelToSearch < freeLists.size(); ++levelToSearch) {
            auto& curList = freeLists[levelToSearch];
            auto entryIt = curList.lower_bound(PageRange{0, numPages});
            if (entryIt != curList.end()) {
                auto entry = *entryIt;
                curList.erase(entryIt);
                --numEntries;
                return splitPageRange(entry, numPages);
            }
        }
    }
    return std::nullopt;
}

PageRange FreeSpaceManager::splitPageRange(PageRange chunk, common::page_idx_t numRequiredPages) {
    KU_ASSERT(chunk.numPages >= numRequiredPages);
    PageRange ret{chunk.startPageIdx, numRequiredPages};
    if (numRequiredPages < chunk.numPages) {
        PageRange remainingEntry{chunk.startPageIdx + numRequiredPages,
            chunk.numPages - numRequiredPages};
        addFreePages(remainingEntry);
    }
    return ret;
}

void FreeSpaceManager::serialize(common::Serializer& ser) const {
    const auto numEntries = getNumEntries();
    ser.writeDebuggingInfo("numEntries");
    ser.write(numEntries);
    ser.writeDebuggingInfo("entries");
    auto entryIt = FreeEntryIterator{freeLists};
    RUNTIME_CHECK(common::row_idx_t numWrittenEntries = 0);
    while (!entryIt.done()) {
        const auto entry = *entryIt;
        ser.write(entry.startPageIdx);
        ser.write(entry.numPages);
        ++entryIt;
        RUNTIME_CHECK(++numWrittenEntries);
    }
    KU_ASSERT(numWrittenEntries == numEntries);
}

void FreeSpaceManager::deserialize(common::Deserializer& deSer) {
    std::string key;

    deSer.validateDebuggingInfo(key, "numEntries");
    common::row_idx_t numEntries{};
    deSer.deserializeValue<common::row_idx_t>(numEntries);

    deSer.validateDebuggingInfo(key, "entries");
    for (common::row_idx_t i = 0; i < numEntries; ++i) {
        PageRange entry{};
        deSer.deserializeValue<common::page_idx_t>(entry.startPageIdx);
        deSer.deserializeValue<common::page_idx_t>(entry.numPages);
        addFreePages(entry);
    }
}

void FreeSpaceManager::finalizeCheckpoint() {
    mergePageRanges(std::move(uncheckpointedFreePageRanges));
    uncheckpointedFreePageRanges.clear();
}

void FreeSpaceManager::resetFreeLists() {
    freeLists.clear();
    numEntries = 0;
}

void FreeSpaceManager::mergePageRanges(free_list_t newInitialEntries) {
    std::vector<PageRange> allEntries = std::move(newInitialEntries);
    for (const auto& freeList : freeLists) {
        allEntries.insert(allEntries.end(), freeList.begin(), freeList.end());
    }
    if (allEntries.empty()) {
        return;
    }

    resetFreeLists();
    std::sort(allEntries.begin(), allEntries.end(), [](const auto& entryA, const auto& entryB) {
        return entryA.startPageIdx < entryB.startPageIdx;
    });

    PageRange prevEntry = allEntries[0];
    for (common::row_idx_t i = 1; i < allEntries.size(); ++i) {
        const auto& entry = allEntries[i];
        if (prevEntry.startPageIdx + prevEntry.numPages == entry.startPageIdx) {
            prevEntry.numPages += entry.numPages;
        } else {
            addFreePages(prevEntry);
            prevEntry = entry;
        }
    }
    addFreePages(prevEntry);
}

common::row_idx_t FreeSpaceManager::getNumEntries() const {
    return numEntries;
}

std::vector<PageRange> FreeSpaceManager::getEntries(common::row_idx_t startOffset,
    common::row_idx_t endOffset) const {
    KU_ASSERT(endOffset >= startOffset);
    std::vector<PageRange> ret;
    FreeEntryIterator it{freeLists};
    it.advance(startOffset);
    while (ret.size() < endOffset - startOffset) {
        KU_ASSERT(!it.done());
        ret.push_back(*it);
        ++it;
    }
    return ret;
}

void FreeEntryIterator::advance(common::row_idx_t numEntries) {
    for (common::row_idx_t i = 0; i < numEntries; ++i) {
        ++(*this);
    }
}

void FreeEntryIterator::operator++() {
    KU_ASSERT(freeListIdx < freeLists.size());
    ++freeListIt;
    if (freeListIt == freeLists[freeListIdx].end()) {
        ++freeListIdx;
        advanceFreeListIdx();
    }
}

bool FreeEntryIterator::done() const {
    return freeListIdx >= freeLists.size();
}

void FreeEntryIterator::advanceFreeListIdx() {
    for (; freeListIdx < freeLists.size(); ++freeListIdx) {
        if (!freeLists[freeListIdx].empty()) {
            freeListIt = freeLists[freeListIdx].begin();
            break;
        }
    }
}

PageRange FreeEntryIterator::operator*() const {
    KU_ASSERT(freeListIdx < freeLists.size() && freeListIt != freeLists[freeListIdx].end());
    return *freeListIt;
}

} // namespace kuzu::storage
