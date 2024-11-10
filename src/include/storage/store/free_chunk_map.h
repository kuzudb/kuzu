#pragma once

#include <vector>
#include <set>

#include "common/constants.h"
#include "common/types/types.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

namespace kuzu {

namespace common {
class Serializer;
class Deserializer;
} // namespace common

namespace storage {

/*
 * FreeChunkLevel indicates how many pages are free to use in a corresponding FreeChunkEntry
 * Note that: these pages are consecutive in disk space and we are indicating the lower limit
 * Therefore, it is possible to waste some fragmented spaces (in FREE_CHUNK_LEVEL_0)
 */
enum FreeChunkLevel : int {
    INVALID_FREE_CHUNK_LEVEL = -1,
    FREE_CHUNK_LEVEL_0 = 0,
    FREE_CHUNK_LEVEL_256 = 1,
    FREE_CHUNK_LEVEL_512 = 2,
    FREE_CHUNK_LEVEL_1024 = 3,
    FREE_CHUNK_LEVEL_2048 = 4,
    FREE_CHUNK_LEVEL_4096 = 5,
    FREE_CHUNK_LEVEL_8192 = 6,
    MAX_FREE_CHUNK_LEVEL = 7
};

/* This const array indicates the upper limit of each level (same as the lower limit of the next level) */
const common::page_idx_t FreeChunkLevelPageNumLimit[MAX_FREE_CHUNK_LEVEL] = {
    256, 512, 1024, 2048, 4096, 8192, UINT32_MAX
};

/*
 * FreeChunkEntry is the main structure to mainain free space information of each chunk:
 *   pageIdx indicates the start page of a given data chunk
 *   numPages indicates how many *consecutive* free pages this data chunk owns
 *   reuseTS is the latest TS when this entry is created. This is to make sure the data of corresponding
 *     data chunk is not recycled until no one keeps TS that is old enough to see it.
 * Note that reuseTS is removed in the 2nd version since flushing only happens when checkpoint and checkpoint will
 * wait all other transactions to finish before proceeding and writting data to disk; with that saying, we are safe
 * to reuse any recycled column chunk here.
 */
typedef struct FreeChunkEntry {
    common::page_idx_t pageIdx;
    common::page_idx_t numPages;
    FreeChunkEntry *nextEntry = nullptr;
} FreeChunkEntry;

/*
 * FreeChunkMap is our main data structure here. It maintains a list of linked list of FreeChunkEntry
 * where the index of each L.L. indicates its FreeChunkLevel and offer necessary interface to its user.
 */
class FreeChunkMap {
public:
    FreeChunkMap();
    ~FreeChunkMap();

    FreeChunkEntry *GetFreeChunk(common::page_idx_t numPages);
    void AddFreeChunk(common::page_idx_t pageIdx, common::page_idx_t numPages);

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<FreeChunkMap> deserialize(common::Deserializer& deserializer,
        main::ClientContext& clientContext);

private:
    FreeChunkLevel GetChunkLevel(common::page_idx_t numPages);
    void UpdateMaxAvailLevel();

    /*No need for locks here since only checkpoint will need free chunks when all other transactions are blocked */
    std::vector<FreeChunkEntry *> freeChunkList;
    std::set<common::page_idx_t> existingFreeChunks;
    FreeChunkLevel maxAvailLevel;
};

} // namespace storage
} // namespace kuzu