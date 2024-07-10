#include "function/gds/ife_morsel.h"

#include "function/table/call_functions.h"

namespace kuzu {
namespace function {

IFEMorsel::~IFEMorsel(){
    delete [] currentFrontier;
    delete [] nextFrontier;
}

void IFEMorsel::init() {
    std::unique_lock lck{mutex};
    if (initializedIFEMorsel) {
        return;
    }
    if (visitedNodes.empty()) {
        visitedNodes = std::vector<uint8_t>(maxOffset + 1, NOT_VISITED_DST);
        pathLength = std::vector<uint8_t>(maxOffset + 1, 0u);
        currentFrontier = new uint8_t[maxOffset + 1]{0u};
        nextFrontier = new uint8_t[maxOffset + 1]{0u};
    } else {
        std::fill(visitedNodes.begin(), visitedNodes.end(), NOT_VISITED_DST);
        std::fill(pathLength.begin(), pathLength.end(), 0u);
        std::fill(currentFrontier, currentFrontier + maxOffset + 1, 0u);
        std::fill(nextFrontier, nextFrontier + maxOffset + 1, 0u);
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    visitedNodes[srcOffset] = VISITED_DST;
    currentFrontier[srcOffset] = 1u;
    currentFrontierSize.fetch_add(1u);
    numVisitedDstNodes.store(1, std::memory_order_relaxed);
}

void IFEMorsel::resetNoLock(common::offset_t srcOffset_) {
    initializedIFEMorsel = false;
    srcOffset = srcOffset_;
}

function::CallFuncMorsel IFEMorsel::getMorsel(uint64_t morselSize) {
    auto morselStartIdx = nextScanStartIdx.fetch_add(morselSize, std::memory_order_acq_rel);
    if (morselStartIdx >= visitedNodes.size()) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto morselEndIdx = std::min(morselStartIdx + morselSize, visitedNodes.size());
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
    if (currentFrontierSize.load(std::memory_order_acq_rel) == 0u) {
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

void IFEMorsel::mergeResults(uint64_t numDstVisitedLocal, uint64_t numNonDstVisitedLocal) {
    numVisitedDstNodes.fetch_add(numDstVisitedLocal);
    nextFrontierSize.fetch_add(numDstVisitedLocal + numNonDstVisitedLocal);
}

void IFEMorsel::initializeNextFrontierNoLock() {
    currentLevel++;
    nextScanStartIdx.store(0LU, std::memory_order_acq_rel);
    currentFrontierSize.store(nextFrontierSize.load(std::memory_order_acq_rel));
    nextFrontierSize.store(0u, std::memory_order_acq_rel);
    auto temp = currentFrontier;
    currentFrontier = nextFrontier;
    nextFrontier = temp;
    std::fill(nextFrontier, nextFrontier + maxOffset + 1, 0u);
}

} // namespace function
} // namespace kuzu
