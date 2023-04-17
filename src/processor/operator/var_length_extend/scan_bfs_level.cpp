#include "processor/operator/var_length_extend/scan_bfs_level.h"

namespace kuzu {
namespace processor {

BFSMorsel::BFSMorsel(common::offset_t maxOffset_) {
    currentBFSLevel = std::make_unique<BFSLevel>();
    nextBFSLevel = std::make_unique<BFSLevel>();
    maxOffset = maxOffset_;
    visitedNodesBuffer = std::make_unique<uint8_t[]>(maxOffset + 1 * sizeof(uint8_t));
}

void BFSMorsel::resetState() {
    std::unique_lock lck{mtx};
    currentLevel = 0;
    currentLevelStartIdx = 0;
    currentBFSLevel->resetState();
    nextBFSLevel->resetState();
    std::fill(visitedNodes, visitedNodes + maxOffset + 1, (uint8_t)VisitedState::NOT_VISITED);
}

bool BFSMorsel::isComplete() {
    if (currentBFSLevel->size() == 0) {
        return true;
    }
    if (currentLevel == upperBound) {
        return true;
    }
    return false;
}

std::unique_ptr<BFSLevelMorsel> BFSMorsel::getBFSLevelMorsel() {
    std::unique_lock lck{mtx};
    if (currentLevelStartIdx == currentBFSLevel->size()) {
        return std::make_unique<BFSLevelMorsel>(currentLevelStartIdx, 0);
    }
    auto size =
        std::min(common::DEFAULT_VECTOR_CAPACITY, currentBFSLevel->size() - currentLevelStartIdx);
    auto morsel = std::make_unique<BFSLevelMorsel>(currentLevelStartIdx, size);
    currentLevelStartIdx += size;
    return morsel;
}

void BFSMorsel::moveNextLevelAsCurrentLevel() {
    currentBFSLevel = std::move(nextBFSLevel);
    currentLevelStartIdx = 0;
    std::sort(currentBFSLevel->nodeOffsets.begin(), currentBFSLevel->nodeOffsets.end());
    nextBFSLevel = std::make_unique<BFSLevel>();
}

void ScanBFSLevel::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {
    tmpSrcNodeIDs->state->selVector->resetSelectorToUnselected();
    // TODO: init node offset
    bfsMorsel = std::make_unique<BFSMorsel>(nodeTable->getMaxNodeOffset(transaction));
}

bool ScanBFSLevel::getNextTuplesInternal(ExecutionContext* context) {
    std::unique_ptr<BFSLevelMorsel> bfsLevelMorsel;
    while (true) {
        if (bfsMorsel->isComplete()) {
            if (!children[0]->getNextTuple(context)) {
                return false;
            }
            bfsMorsel->resetState();
            assert(srcNodeIDs->state->isFlat());
            auto nodeID = srcNodeIDs->getValue<common::nodeID_t>(
                srcNodeIDs->state->selVector->selectedPositions[0]);
            bfsMorsel->currentBFSLevel->nodeOffsets.push_back(nodeID.offset);
        }
        bfsLevelMorsel = bfsMorsel->getBFSLevelMorsel();
        if (!bfsLevelMorsel->empty()) { // Found a BFS level morsel.
            break;
        } else { // Otherwise
            bfsMorsel->moveNextLevelAsCurrentLevel();
        }
    }
    writeBFSLevelMorselToTmpSrcNodeIDsVector(
        bfsLevelMorsel.get(), bfsMorsel->currentBFSLevel.get());
    return true;
}

void ScanBFSLevel::writeBFSLevelMorselToTmpSrcNodeIDsVector(
    BFSLevelMorsel* bfsLevelMorsel, BFSLevel* bfsLevel) {
    for (auto i = 0; i < bfsLevelMorsel->size; ++i) {}
}

} // namespace processor
} // namespace kuzu