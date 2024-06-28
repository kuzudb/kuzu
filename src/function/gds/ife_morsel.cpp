#include "function/gds/ife_morsel.h"

#include "function/table/call_functions.h"

namespace kuzu {
namespace function {

IFEMorsel::~IFEMorsel() {
    delete [] visitedNodes;
    delete [] pathLength;
}

void IFEMorsel::init() {
    if (initializedIFEMorsel) {
        return;
    }
    if (!visitedNodes) {
        visitedNodes = new std::atomic<uint8_t>[maxOffset + 1] { NOT_VISITED_DST };
        pathLength = new std::atomic<uint16_t>[maxOffset + 1] { 0u };
    } else {
        for (auto i = 0u; i < maxOffset + 1; i++) {
            visitedNodes[i].store(NOT_VISITED_DST, std::memory_order_relaxed);
            pathLength[i].store(0u, std::memory_order_relaxed);
        }
    }
    initializedIFEMorsel = true;
    currentLevel = 0u;
    nextScanStartIdx.store(0u, std::memory_order_relaxed);
    nextDstScanStartIdx.store(0u, std::memory_order_relaxed);
    visitedNodes[srcOffset].store(VISITED_DST, std::memory_order_relaxed);
    numVisitedDstNodes.store(1, std::memory_order_relaxed);
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
    nextScanStartIdx = 0LU;
    if (isBFSCompleteNoLock()) {
        return;
    }
    bfsLevelNodeOffsets.clear();
    for (auto offset = 0u; offset < (maxOffset + 1); offset++) {
        auto state = visitedNodes[offset].load(std::memory_order_acq_rel);
        if (state == VISITED_DST_NEW) {
            bfsLevelNodeOffsets.push_back(offset);
            visitedNodes[offset].store(VISITED_DST, std::memory_order_acq_rel);
        } else if (state == VISITED_NEW) {
            bfsLevelNodeOffsets.push_back(offset);
            visitedNodes[offset].store(VISITED, std::memory_order_acq_rel);
        }
    }
}

} // namespace graph
} // namespace kuzu
