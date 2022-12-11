#include "processor/operator/shortest_path_adj_list.h"

#include "iostream"

using namespace std;

namespace kuzu {
namespace processor {

bool ShortestPathAdjList::getNextTuplesInternal() {
    while (true) {
        computeShortestPath();
        uint64_t currSrcIdx;
        uint64_t currDestIdx;
        do {
            if (!children[0]->getNextTuple()) {
                return false;
            }
            currSrcIdx = srcValueVector->state->getPositionOfCurrIdx();
            currDestIdx = destValueVector->state->getPositionOfCurrIdx();
        } while (srcValueVector->isNull(currSrcIdx) || destValueVector->isNull(currDestIdx) ||
                 !addBFSLevel(srcValueVector->readNodeOffset(currSrcIdx), 1, UINT64_MAX));
    }
}

void ShortestPathAdjList::computeShortestPath() {
    // If queue is not empty, meaning source node has been added.
    while (!bfsQueue.empty()) {
        auto queueFrontNodeOffset = bfsQueue.front();
        auto bfsFrontNodeState = bfsVisitedNodesMap[queueFrontNodeOffset];
        if (bfsFrontNodeState->bfsLevel >= lowerBound &&
            bfsFrontNodeState->bfsLevel <= upperBound && bfsFrontNodeState->hasBeenOutput) {
            auto destNodeOffset = destValueVector->readNodeOffset(0);
            if (bfsVisitedNodesMap.contains(destValueVector->readNodeOffset(0))) {
                do {
                    cout << destNodeOffset << " ";
                    destNodeOffset = bfsVisitedNodesMap[destNodeOffset]->parentNodeID;
                } while (destNodeOffset != UINT64_MAX);
                cout << endl;
            }
            bfsFrontNodeState->hasBeenOutput = false;
        } else if (!bfsFrontNodeState->hasBeenOutput &&
                   bfsFrontNodeState->currChildIdx <
                       bfsFrontNodeState->children->state->selVector->selectedSize &&
                   bfsFrontNodeState->bfsLevel <= upperBound) {
            auto totalChildNodes = bfsFrontNodeState->children->state->selVector->selectedSize;
            uint64_t i;
            for (i = bfsFrontNodeState->currChildIdx; i < totalChildNodes; i++) {
                addBFSLevel(bfsFrontNodeState->children->readNodeOffset(i),
                    bfsFrontNodeState->bfsLevel + 1, queueFrontNodeOffset);
            }
            bfsFrontNodeState->currChildIdx += i;
            bfsFrontNodeState->hasBeenOutput = true;
        } else if (getNextBatchOfChildNodes(bfsFrontNodeState.get())) {
            bfsFrontNodeState->hasBeenOutput = false;
            bfsFrontNodeState->currChildIdx = 0;
        } else {
            bfsQueue.pop();
        }
    }
    // Reset map after all nodes have been visited and queue is empty.
    bfsVisitedNodesMap = map<uint64_t, shared_ptr<BFSState>>();
}

bool ShortestPathAdjList::addBFSLevel(uint64_t parent, uint64_t bfsLevel, uint64_t predecessor) {
    if (!bfsVisitedNodesMap.contains(parent)) {
        auto listSyncState = make_shared<ListSyncState>();
        auto listHandle = make_shared<ListHandle>(*listSyncState);
        ((AdjLists*)storage)->initListReadingState(parent, *listHandle, transaction->getType());
        auto children = make_shared<ValueVector>(NODE_ID, memoryManager);
        children->state = make_shared<DataChunkState>();
        ((AdjLists*)storage)->readValues(children, *listHandle);
        bfsQueue.emplace(parent);
        auto newBFSState = make_shared<BFSState>(predecessor, bfsLevel, children,
            0 /* currChildIdx*/, false /* hasBeenOutput*/, listSyncState, listHandle);
        bfsVisitedNodesMap.emplace(parent, newBFSState);
        return true;
    } else {
        return false;
    }
}

bool ShortestPathAdjList::getNextBatchOfChildNodes(BFSState* bfsState) {
    if (bfsState->listHandle->listSyncState.hasMoreToRead()) {
        ((AdjLists*)storage)->readValues(bfsState->children, *bfsState->listHandle);
        return true;
    }
    return false;
}

} // namespace processor
} // namespace kuzu
