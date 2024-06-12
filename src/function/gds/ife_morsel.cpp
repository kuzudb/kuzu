#include "function/gds/ife_morsel.h"

#include "function/table/call_functions.h"

namespace kuzu {
namespace function {

void IFEMorsel::initSourceNoLock(common::offset_t srcOffset) {
    visitedNodes[srcOffset].store(VISITED_DST, std::memory_order_acq_rel);
    numVisitedDstNodes.fetch_add(1);
    bfsLevelNodeOffsets.push_back(srcOffset);
}

function::CallFuncMorsel IFEMorsel::getMorsel(uint64_t& morselSize) {
    auto curStartIdx = nextScanStartIdx.load(std::memory_order_acq_rel);
    if (curStartIdx >= bfsLevelNodeOffsets.size()) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto nextStartIdx = std::min(curStartIdx + morselSize, bfsLevelNodeOffsets.size());
    while (!nextScanStartIdx.compare_exchange_strong(curStartIdx, nextStartIdx,
        std::memory_order_acq_rel)) {
        nextStartIdx = std::min(curStartIdx + morselSize, bfsLevelNodeOffsets.size());
    }
    return {curStartIdx, nextStartIdx};
}

function::CallFuncMorsel IFEMorsel::getDstWriteMorsel(uint64_t& morselSize) {
    auto curStartIdx = nextDstScanStartIdx.load(std::memory_order_acq_rel);
    if (curStartIdx >= visitedNodes.size()) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto nextStartIdx = std::min(curStartIdx + morselSize, visitedNodes.size());
    while (!nextDstScanStartIdx.compare_exchange_strong(curStartIdx, nextStartIdx,
        std::memory_order_acq_rel)) {
        nextStartIdx = std::min(curStartIdx + morselSize, visitedNodes.size());
    }
    return {curStartIdx, nextStartIdx};
}

bool IFEMorsel::isCompleteNoLock() const {
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

void IFEMorsel::mergeResults(uint64_t numDstVisitedLocal) {
    numVisitedDstNodes.fetch_add(numDstVisitedLocal, std::memory_order_acq_rel);
}

void IFEMorsel::initializeNextFrontierNoLock() {
    currentLevel++;
    nextScanStartIdx = 0LU;
    if (isCompleteNoLock()) {
        return;
    }
    bfsLevelNodeOffsets.clear();
    for (auto offset = 0u; offset < visitedNodes.size(); offset++) {
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
