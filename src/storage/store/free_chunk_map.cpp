#include "storage/store/free_chunk_map.h"


#include "common/assert.h"
#include "common/constants.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
using namespace kuzu::common;

namespace kuzu {
namespace storage {

FreeChunkMap::FreeChunkMap()
    :maxAvailLevel(INVALID_FREE_CHUNK_LEVEL) {
    /* we initialize our vector as nullptr vector by default */
    for (int i = 0; i < MAX_FREE_CHUNK_LEVEL; i++) {
        freeChunkList.push_back(nullptr);
    }
    maxAvailLevel = INVALID_FREE_CHUNK_LEVEL;
};

FreeChunkMap::~FreeChunkMap() {
    for (int i = 0; i < MAX_FREE_CHUNK_LEVEL; i++) {
        while (freeChunkList[i] != nullptr) {
            FreeChunkEntry *curEntry = freeChunkList[i];
            freeChunkList[i] = curEntry->nextEntry;
            free(curEntry);
        }
    }

    freeChunkList.clear();
    maxAvailLevel = INVALID_FREE_CHUNK_LEVEL;
}

/*
 * This function returns which FREE_CHUNK_LEVEL a given numPages belongs to.
 * For example,
 *     numPages <= 256  -> FREE_CHUNK_LEVEL_0
 *     numPages <= 512  -> FREE_CHUNK_LEVEL_256
 *     numPages <= 1024 -> FREE_CHUNK_LEVEL_512
 *     numPages <= 2048 -> FREE_CHUNK_LEVEL_1024
 *     ...
 */
FreeChunkLevel FreeChunkMap::GetChunkLevel(common::page_idx_t numPages)
{
    /* if numPage <= ith FreeChunkLevelPageNumLimit, we put it at FREE_CHUNK_LEVEL_i */
    for (int i = FREE_CHUNK_LEVEL_0; i < MAX_FREE_CHUNK_LEVEL; i++) {
        if (numPages <= FreeChunkLevelPageNumLimit[i]) {
            return static_cast<FreeChunkLevel>(i);
        }
    }

    /* You should never reach here since FreeChunkLevelPageNumLimit has last entry as UINT32_MAX */
    KU_ASSERT(false);
    return MAX_FREE_CHUNK_LEVEL;
}

/*
 * Note: Any caller of this function need to add the entry back to FreeChunkMap after use so that the rest
 * of its unused space will be reused
 */
FreeChunkEntry *FreeChunkMap::GetFreeChunk(common::page_idx_t numPages, int64_t reuseTS)
{
    /* 1. Get the corresponding ChunkLevel numPages belongs to */
    FreeChunkLevel curLevel = GetChunkLevel(numPages);
    KU_ASSERT(curLevel < MAX_FREE_CHUNK_LEVEL);

    /* 2. return nullptr if have no entry for the given level and above */
    if (maxAvailLevel < curLevel) {
        return nullptr;
    }

    /* 3. Now, search an usable entry for given numPages and timestamp unitl exceeding the max level */
    while (curLevel <= maxAvailLevel) {
        /* if the level we are searching has no space to reuse, continue search the next level */
        if (freeChunkList[curLevel] == nullptr) {
            curLevel = static_cast<FreeChunkLevel>((curLevel + 1 < maxAvailLevel)? curLevel + 1:maxAvailLevel);
            continue;
        }

        /* Search the current level for an entry with valid reuse timestamp */
        KU_ASSERT(freeChunkList[curLevel] != nullptr);
        FreeChunkEntry *curEntry = freeChunkList[curLevel];
        FreeChunkEntry *lastSearchEntry = nullptr;
        while (curEntry != nullptr) {
            if (curEntry->numPages >= numPages && curEntry->reuseTS <= reuseTS) {
                /* found a valid entry to reuse. Remove it from current linked list */
                if (lastSearchEntry == nullptr) {
                    /* the valid entry is the first entry in the L.L. */
                    freeChunkList[curLevel] = curEntry->nextEntry;

                    /* update maxAvailLevel if needed */
                    /* ERICTODO: Change this to be a helper function */
                    if (curLevel == maxAvailLevel && freeChunkList[curLevel] == nullptr) {
                        FreeChunkLevel nextAvailLevel = INVALID_FREE_CHUNK_LEVEL;
                        for (int i = curLevel; i >= FREE_CHUNK_LEVEL_0; i--) {
                            if (freeChunkList[i] != nullptr) {
                                nextAvailLevel = static_cast<FreeChunkLevel>(i);
                                break;
                            }
                        }
                        maxAvailLevel = nextAvailLevel;
                    }
                } else {
                    /* need to unlink it from its parent */
                    lastSearchEntry->nextEntry = curEntry->nextEntry;
                }

                curEntry->nextEntry = nullptr;
                return curEntry;
            }

            /* Move to the next entry */
            lastSearchEntry = curEntry;
            curEntry = curEntry->nextEntry;
        }
    }
    
    /* No reusable chunk. Just return nullptr here */
    return nullptr;
}

void FreeChunkMap::AddFreeChunk(common::page_idx_t pageIdx, common::page_idx_t numPages, int64_t reuseTS)
{
    /* 1. Get the corresponding ChunkLevel numPages belongs to */
    FreeChunkLevel curLevel = GetChunkLevel(numPages);
    KU_ASSERT(curLevel < MAX_FREE_CHUNK_LEVEL);

    /* 2. Create a new FreeChunkEntry */
    FreeChunkEntry *entry = (FreeChunkEntry *)malloc(sizeof(FreeChunkEntry));
    entry->pageIdx = pageIdx;
    entry->numPages = numPages;
    entry->reuseTS = reuseTS;

    /* 3. Insert it into the L.L. */
    if (freeChunkList[curLevel] == nullptr) {
        freeChunkList[curLevel] = entry;
        if (maxAvailLevel < curLevel) {
            maxAvailLevel = curLevel;
        }
    } else {
        FreeChunkEntry *curEntryInList = freeChunkList[curLevel];
        while (curEntryInList->nextEntry != nullptr) {
            curEntryInList = curEntryInList->nextEntry;
        }

        KU_ASSERT(curEntryInList != nullptr);
        curEntryInList->nextEntry = entry;
    }
}

/* ERICTODO: Implement this for data persistency */
void FreeChunkMap::serialize(Serializer& serializer) const
{
    (void) serializer;
    return;
}

/* ERICTODO: Implement this for data persistency */
static std::unique_ptr<FreeChunkMap> deserialize(Deserializer& deserializer,
        main::ClientContext& clientContext)
{
    (void) deserializer;
    (void) clientContext;
    return nullptr;
}

} // namespace storage
} // namespace kuzu
