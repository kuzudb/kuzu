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
};

struct IFEMorsel {
public:
    IFEMorsel(uint64_t upperBound_, uint64_t lowerBound_, uint64_t maxNodeOffset_,
        common::offset_t srcOffset)
        : mutex{std::mutex()}, initializedIFEMorsel{false}, currentLevel{0u}, nextScanStartIdx{0u},
          currentFrontierSize{0u}, srcOffset{srcOffset}, numVisitedDstNodes{0u},
          numDstNodesToVisit{maxNodeOffset_ + 1}, maxOffset{maxNodeOffset_},
          upperBound{upperBound_}, lowerBound{lowerBound_}, nextDstScanStartIdx{0u} {}

    ~IFEMorsel();

    void init();

    void resetNoLock(common::offset_t srcOffset);

    function::CallFuncMorsel getMorsel(uint64_t morselSize);

    function::CallFuncMorsel getDstWriteMorsel(uint64_t morselSize);

    bool isBFSCompleteNoLock() const;

    bool isIFEMorselCompleteNoLock() const;

    void mergeResults(uint64_t numDstVisitedLocal, uint64_t numNonDstVisitedLocal);

    void initializeNextFrontierNoLock();

public:
    std::mutex mutex;
    bool initializedIFEMorsel;
    uint8_t currentLevel;
    char padding0[64]{0};
    std::atomic<uint64_t> nextScanStartIdx;
    char padding1[64]{0};
    std::atomic<uint64_t> currentFrontierSize;
    char padding2[64]{0};
    std::atomic<uint64_t> nextFrontierSize;
    char padding3[64]{0};
    common::offset_t srcOffset;

    // Visited state
    char padding4[64]{0};
    std::atomic<uint64_t> numVisitedDstNodes;
    char padding5[64]{0};
    uint64_t numDstNodesToVisit;
    char padding6[64]{0};
    std::vector<uint8_t> visitedNodes;
    uint8_t *currentFrontier;
    uint8_t *nextFrontier;
    std::vector<uint8_t> pathLength;
    // Maximum offset of dst nodes.
    common::offset_t maxOffset;
    uint64_t upperBound;
    uint64_t lowerBound;
    std::atomic<uint64_t> nextDstScanStartIdx;
};

} // namespace function
} // namespace kuzu
