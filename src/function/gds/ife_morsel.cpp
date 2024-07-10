#include "function/gds/ife_morsel.h"

#include "function/table/call_functions.h"

namespace kuzu {
namespace function {

void IFEMorsel::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    if (visitedNodes.empty()) {
        visitedNodes = std::vector<uint8_t>(maxOffset + 1, NOT_VISITED_DST);
        pathLength = std::vector<uint16_t>(maxOffset + 1, 0u);
    } else {
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED_DST);
        std::fill(pathLength.begin(), pathLength.end(), 0u);
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    visitedNodes[srcOffset] = VISITED_DST;
    numVisitedDstNodes.store(1, std::memory_order_relaxed);
    bfsLevelNodeOffsets.clear();
    bfsLevelNodeOffsets.push_back(srcOffset);
}

void IFEMorsel::resetNoLock(common::offset_t srcOffset_) {
    initializedIFEMorsel = false;
    srcOffset = srcOffset_;
}

function::CallFuncMorsel IFEMorsel::getMorsel(uint64_t morselSize) {
    auto morselStartIdx = nextScanStartIdx.fetch_add(morselSize, std::memory_order_acq_rel);
    if (morselStartIdx >= bfsLevelNodeOffsets.size()) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto morselEndIdx = std::min(morselStartIdx + morselSize, bfsLevelNodeOffsets.size());
    return {morselStartIdx, morselEndIdx};
}

function::CallFuncMorsel IFEMorsel::getDstWriteMorsel(uint64_t morselSize) {
    auto morselStartIdx = nextDstScanStartIdx.fetch_add(morselSize, std::memory_order_acq_rel);
    if (morselStartIdx >= maxOffset) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto morselEndIdx = std::min(morselStartIdx + morselSize, maxOffset + 1);
    return {morselStartIdx, morselEndIdx};
}

bool IFEMorsel::isBFSCompleteNoLock() const {
    if (currentLevel == upperBound) {
        return true;
    }
    if (bfsLevelNodeOffsets.empty()) {
        return true;
    }
    if (numVisitedDstNodes.load(std::memory_order_acq_rel) == numDstNodesToVisit) {
        return true;
    }
    return false;
}

bool IFEMorsel::isIFEMorselCompleteNoLock() const {
    return isBFSCompleteNoLock() &&
           (nextDstScanStartIdx.load(std::memory_order_acq_rel) >= maxOffset);
}

void IFEMorsel::mergeResults(uint64_t numDstVisitedLocal) {
    numVisitedDstNodes.fetch_add(numDstVisitedLocal, std::memory_order_acq_rel);
}

void IFEMorsel::initializeNextFrontierNoLock() {
    currentLevel++;
    nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
    if (isBFSCompleteNoLock()) {
        return;
    }
    bfsLevelNodeOffsets.clear();
    for (auto offset = 0u; offset < (maxOffset + 1); offset++) {
        auto nodeState = visitedNodes[offset];
        if (nodeState == VISITED_DST_NEW) {
            bfsLevelNodeOffsets.push_back(offset);
            visitedNodes[offset]= VISITED_DST;
        } else if (nodeState == VISITED_NEW) {
            bfsLevelNodeOffsets.push_back(offset);
            visitedNodes[offset] = VISITED;
        }
    }
}

} // namespace function
} // namespace kuzu
