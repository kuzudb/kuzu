#pragma once

#include <atomic>
#include <mutex>
#include <vector>

#include "common/types/internal_id_t.h"
#include "function/table/call_functions.h"

namespace kuzu {
namespace graph {

enum VisitedState : uint8_t {
    NOT_VISITED_DST = 0,
    VISITED_DST = 1,
    NOT_VISITED = 2,
    VISITED = 3,
    VISITED_NEW = 4,
    VISITED_DST_NEW = 5,
};

struct IFEMorsel {
public:
    IFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_)
        : mutex{std::mutex()}, currentLevel{0u}, nextScanStartIdx{0u}, numVisitedDstNodes{0u},
          numDstNodesToVisit{maxNodeOffset_ + 1},
          visitedNodes{std::vector<std::atomic<uint8_t>>(maxNodeOffset_ + 1)},
          pathLength{std::vector<std::atomic<uint16_t>>(maxNodeOffset_ + 1)},
          bfsLevelNodeOffsets{std::vector<common::offset_t>()}, maxOffset{maxNodeOffset_},
          upperBound{upperBound_}, lowerBound{lowerBound_}, nextDstScanStartIdx{0u} {
        for (auto i = 0u; i < maxNodeOffset_ + 1; i++) {
            visitedNodes[i].store(NOT_VISITED_DST, std::memory_order::relaxed);
            pathLength[i].store(0u, std::memory_order_relaxed);
        }
    }

    void initSourceNoLock(common::offset_t srcOffset);

    function::CallFuncMorsel getMorsel(uint64_t &morselSize);

    function::CallFuncMorsel getDstWriteMorsel(uint64_t& morselSize);

    bool isCompleteNoLock() const;

    void mergeResults(uint64_t numDstVisitedLocal);

    void initializeNextFrontierNoLock();

public:
    std::mutex mutex;
    uint8_t currentLevel;
    uint64_t nextScanStartIdx;

    // Visited state
    uint64_t numVisitedDstNodes;
    uint64_t numDstNodesToVisit;
    std::vector<std::atomic_uint8_t> visitedNodes;
    std::vector<std::atomic_uint16_t> pathLength;
    std::vector<common::offset_t> bfsLevelNodeOffsets;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    uint64_t nextDstScanStartIdx;
};

}
}
