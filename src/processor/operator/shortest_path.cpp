#include "processor/operator/shortest_path.h"

#include "iostream"

using namespace std;

namespace kuzu {
namespace processor {

bool ShortestPathAdjList::getNextTuplesInternal() {
    while (true) {
        while (!bfsQueue.empty()) {
            auto queueFrontNodeOffset = bfsQueue.front();
            auto bfsFrontNodeState = bfsVisitedNodesMap[queueFrontNodeOffset];
            if (bfsFrontNodeState->bfsLevel >= lowerBound &&
                bfsFrontNodeState->bfsLevel <= upperBound && bfsFrontNodeState->hasBeenOutput) {
                for (auto i = destValueVector->state->getPositionOfCurrIdx();
                     i < destValueVector->state->selVector->selectedSize; i++) {
                    if (bfsVisitedNodesMap.contains(destValueVector->readNodeOffset(i))) {
                        auto currNode = destValueVector->readNodeOffset(i);
                        do {
                            cout << currNode << " ";
                            currNode = bfsVisitedNodesMap[currNode]->parentNodeID;
                        } while (currNode != UINT64_MAX);
                        cout << endl;
                    }
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

bool ShortestPathAdjList::addBFSLevel(uint64_t parent, uint64_t bfsLevel, uint64_t predecessor) {
    if (!bfsVisitedNodesMap.contains(parent)) {
        auto newBFSState = make_shared<BFSState>();
        newBFSState->listSyncState = make_shared<ListSyncState>();
        newBFSState->listHandle = make_shared<ListHandle>(*newBFSState->listSyncState);
        adjList->initListReadingState(parent, *newBFSState->listHandle, transaction->getType());
        auto children = make_shared<ValueVector>(NODE_ID, memoryManager);
        children->state = make_shared<DataChunkState>();
        adjList->readValues(children, *newBFSState->listHandle);
        newBFSState->bfsLevel = bfsLevel;
        newBFSState->children = children;
        newBFSState->currChildIdx = 0;
        newBFSState->hasBeenOutput = false;
        newBFSState->parentNodeID = predecessor;
        bfsQueue.emplace(parent);
        bfsVisitedNodesMap.emplace(parent, newBFSState);
        return true;
    } else {
        return false;
    }
}

bool ShortestPathAdjList::getNextBatchOfChildNodes(BFSState* bfsState) {
    if (bfsState->listHandle->listSyncState.hasMoreToRead()) {
        adjList->readValues(bfsState->children, *bfsState->listHandle);
        return true;
    }
    return false;
}

bool ShortestPathAdjCol::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    cout << ((int64_t*)(srcValueVector->getData()))[0] << endl;
    cout << ((int64_t*)(destValueVector->getData()))[0] << endl;
    return true;
}
} // namespace processor
} // namespace kuzu
