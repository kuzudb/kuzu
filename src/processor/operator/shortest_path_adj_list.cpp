#include "processor/operator/shortest_path_adj_list.h"

#include "iostream"

using namespace std;

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> ShortestPathAdjList::init(ExecutionContext* context) {
    resultSet = BaseShortestPath::init(context);
    relListHandles = make_shared<ListHandle>(*listSyncState);
    return resultSet;
}

bool ShortestPathAdjList::getNextTuplesInternal() {
    while (true) {
        if (!children[0]->getNextTuple()) {
            return false;
        }
        uint64_t currIdx = srcValueVector->state->currIdx;
        uint64_t destIdx = destValueVector->state->currIdx;
        if (srcValueVector->isNull(currIdx) || destValueVector->isNull(destIdx)) {
            continue;
        }
        if (computeShortestPath(currIdx, destIdx)) {
            return true;
        }
    }
}

bool ShortestPathAdjList::computeShortestPath(uint64_t currIdx, uint64_t destIdx) {
    bfsVisitedNodesMap = map<uint64_t, shared_ptr<NodeState>>();
    currFrontier = make_shared<vector<node_offset_t>>();
    nextFrontier = make_shared<vector<node_offset_t>>();
    listHandle->reset();

    uint64_t currLevel = 1, destNodeOffset;
    if (!visitNextFrontierNode(srcValueVector->readNodeOffset(currIdx), UINT64_MAX, INT64_MAX)) {
        return false;
    }
    currFrontier->push_back(srcValueVector->readNodeOffset(currIdx));

    while (currLevel <= upperBound) {
        destNodeOffset =
            currLevel < lowerBound ? UINT64_MAX : destValueVector->readNodeOffset(destIdx);
        if (extendToNextFrontier(destNodeOffset)) {
            printShortestPath(destNodeOffset);
            return true;
        }
        currLevel++;
        resetFrontier();
    }
    return false;
}

bool ShortestPathAdjList::extendToNextFrontier(node_offset_t destNodeOffset) {
    NodeState* nodeState;
    for (auto i = 0u; i < currFrontier->size(); i++) {
        nodeState = bfsVisitedNodesMap[currFrontier->operator[](i)].get();
        do {
            for (auto j = 0u; j < nodeState->children->state->selVector->selectedSize; j++) {
                if (addToNextFrontier(nodeState, j, currFrontier->operator[](i), destNodeOffset)) {
                    return true;
                }
            }
        } while (getNextBatchOfChildNodes(nodeState));
    }
    return false;
}

bool ShortestPathAdjList::addToNextFrontier(NodeState*& nodeState, uint64_t index,
    node_offset_t parentNodeOffset, node_offset_t destNodeOffset) {
    node_offset_t nextFrontierNodeOffset = nodeState->children->readNodeOffset(index);
    if (visitNextFrontierNode(nodeState->children->readNodeOffset(index), parentNodeOffset,
            nodeState->relIDVector->getValue<int64_t>(index))) {
        nextFrontier->push_back(nextFrontierNodeOffset);
        if (nextFrontierNodeOffset == destNodeOffset) {
            return true;
        }
    }
    return false;
}

bool ShortestPathAdjList::visitNextFrontierNode(
    uint64_t nodeOffset, uint64_t predecessor, int64_t parentRelID) {
    if (!bfsVisitedNodesMap.contains(nodeOffset)) {
        lists->initListReadingState(nodeOffset, *listHandle, transaction->getType());
        auto children = make_shared<ValueVector>(NODE_ID, memoryManager);
        auto relVector = make_shared<ValueVector>(INT64, memoryManager);
        children->state = make_shared<DataChunkState>();
        relVector->state = make_shared<DataChunkState>();
        lists->readValues(children, *listHandle);
        relVector->state->initOriginalAndSelectedSize(children->state->selVector->selectedSize);
        relPropertyLists->readValues(relVector, *relListHandles);
        auto newBFSState = make_shared<NodeState>(predecessor, parentRelID, children, relVector);
        bfsVisitedNodesMap.emplace(nodeOffset, newBFSState);
        return true;
    } else {
        return false;
    }
}

bool ShortestPathAdjList::getNextBatchOfChildNodes(NodeState* bfsState) {
    if (listHandle->listSyncState.hasMoreToRead()) {
        lists->readValues(bfsState->children, *listHandle);
        return true;
    }
    return false;
}

void ShortestPathAdjList::resetFrontier() {
    currFrontier = move(nextFrontier);
    nextFrontier = make_shared<vector<node_offset_t>>();
}

void ShortestPathAdjList::printShortestPath(node_offset_t destNodeOffset) {
    do {
        cout << destNodeOffset << " ";
        if (bfsVisitedNodesMap[destNodeOffset]->relParentID != INT64_MAX) {
            cout << bfsVisitedNodesMap[destNodeOffset]->relParentID << " ";
        }
        destNodeOffset = bfsVisitedNodesMap[destNodeOffset]->parentNodeID;
    } while (destNodeOffset != UINT64_MAX);
    cout << endl;
}

} // namespace processor
} // namespace kuzu
