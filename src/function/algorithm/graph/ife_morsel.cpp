#include "function/algorithm/ife_morsel.h"
#include "function/table/call_functions.h"

namespace kuzu {
namespace graph {

void IFEMorsel::initSourceNoLock(common::offset_t srcOffset) {
    visitedNodes[srcOffset].store(VISITED_DST, std::memory_order_acq_rel);
    numVisitedDstNodes++;
    bfsLevelNodeOffsets.push_back(srcOffset);
}

function::CallFuncMorsel IFEMorsel::getMorsel(uint64_t &morselSize) {
    std::unique_lock lck{mutex};
    if (nextScanStartIdx >= bfsLevelNodeOffsets.size()) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto startIdx = nextScanStartIdx;
    auto endIdx = std::min(bfsLevelNodeOffsets.size(), nextScanStartIdx + morselSize);
    nextScanStartIdx += morselSize;
    return {startIdx, endIdx};
}

function::CallFuncMorsel IFEMorsel::getDstWriteMorsel(uint64_t& morselSize) {
    std::unique_lock lck{mutex};
    if (nextDstScanStartIdx >= visitedNodes.size()) {
        return function::CallFuncMorsel::createInvalidMorsel();
    }
    auto startIdx = nextDstScanStartIdx;
    auto endIdx = std::min(visitedNodes.size(), nextDstScanStartIdx + morselSize);
    nextDstScanStartIdx += morselSize;
    return {startIdx, endIdx};
}

bool IFEMorsel::isCompleteNoLock() const {
    if (currentLevel == upperBound) {
        return true;
    }
    if (bfsLevelNodeOffsets.empty()) {
        return true;
    }
    if (numVisitedDstNodes == numDstNodesToVisit) {
        return true;
    }
    return false;
}

void IFEMorsel::mergeResults(uint64_t numDstVisitedLocal) {
    std::unique_lock lck{mutex};
    numVisitedDstNodes += numDstVisitedLocal;
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

}
}
