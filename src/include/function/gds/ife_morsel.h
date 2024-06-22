#pragma once

#include <atomic>
#include <mutex>
#include <vector>

#include "common/types/internal_id_t.h"
#include "function/table/call_functions.h"

namespace kuzu {
namespace function {

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
    IFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_,
        common::offset_t srcOffset)
        : mutex{std::mutex()}, initializedIFEMorsel{false}, currentLevel{0u},
          nextScanStartIdx{0u}, srcOffset{srcOffset}, numVisitedDstNodes{0u},
          numDstNodesToVisit{maxNodeOffset_ + 1}, visitedNodes{nullptr}, pathLength{nullptr},
          bfsLevelNodeOffsets{std::vector<common::offset_t>()}, maxOffset{maxNodeOffset_},
          upperBound{upperBound_}, lowerBound{lowerBound_}, nextDstScanStartIdx{0u} {}

    ~IFEMorsel();

    void init();

    void resetNoLock(common::offset_t srcOffset);

    function::CallFuncMorsel getMorsel(uint64_t morselSize);

    function::CallFuncMorsel getDstWriteMorsel(uint64_t morselSize);

    bool isCompleteNoLock() const;

    void mergeResults(uint64_t numDstVisitedLocal);

    void initializeNextFrontierNoLock();

public:
    std::mutex mutex;
    bool initializedIFEMorsel;
    uint8_t currentLevel;
    std::atomic<uint64_t> nextScanStartIdx;
    common::offset_t srcOffset;

    // Visited state
    std::atomic<uint64_t> numVisitedDstNodes;
    uint64_t numDstNodesToVisit;
    std::atomic<uint8_t>* visitedNodes;
    std::atomic<uint16_t>* pathLength;
    std::vector<common::offset_t> bfsLevelNodeOffsets;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    std::atomic<uint64_t> nextDstScanStartIdx;
};

} // namespace function
} // namespace kuzu
