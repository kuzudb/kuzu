#pragma once

#include "lists.h"

namespace graphflow {
namespace storage {

class ListUpdateIterator {
public:
    ListUpdateIterator(Lists* lists) : lists{lists} {}

    void seekToChunkIdx(uint64_t chunkIdx) {}

    void seekToListIdxAndSlideIfNecessary(uint64_t listIdx);

    void updateCurrentListIdx(uint8_t* newList, uint64_t size);

    Lists* lists;
};

} // namespace storage
} // namespace graphflow
