#include "processor/operator/shortest_path_adj_list.h"

#include "iostream"

using namespace std;

namespace kuzu {
namespace processor {

void ShortestPathAdjList::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    BaseShortestPath::initLocalStateInternal(resultSet, context);
    adjNodeIDVector = make_shared<ValueVector>(NODE_ID, context->memoryManager);
    adjNodeIDVector->state = make_shared<DataChunkState>();
    relIDVector = make_shared<ValueVector>(INT64, context->memoryManager);
    relIDVector->state = adjNodeIDVector->state;
    relListHandles = make_shared<ListHandle>(*listSyncState);
}

bool ShortestPathAdjList::getNextTuplesInternal() {
    while (true) {
        if (!children[0]->getNextTuple()) {
            return false;
        }
        uint64_t srcIdx = srcValueVector->state->selVector->selectedPositions[0];
        uint64_t destIdx = destValueVector->state->selVector->selectedPositions[0];
        if (srcValueVector->isNull(srcIdx) || destValueVector->isNull(destIdx)) {
            continue;
        }
        if (computeShortestPath(srcIdx, destIdx)) {
            return true;
        }
    }
}

bool ShortestPathAdjList::computeShortestPath(uint64_t currIdx, uint64_t destIdx) {
    bfsVisitedNodesMap = map<uint64_t, unique_ptr<NodeState>>();
    currFrontier = make_shared<vector<node_offset_t>>();
    nextFrontier = make_shared<vector<node_offset_t>>();
    listHandle->reset();

    uint64_t currLevel = 1, destNodeOffset;
    auto srcNodeOffset = srcValueVector->readNodeOffset(currIdx);
    currFrontier->push_back(srcNodeOffset);
    bfsVisitedNodesMap[srcNodeOffset] = make_unique<NodeState>(UINT64_MAX, INT64_MAX);

    while (currLevel < upperBound) {
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
    uint64_t nodeOffset;
    for (auto i = 0u; i < currFrontier->size(); i++) {
        nodeOffset = currFrontier->operator[](i);
        lists->initListReadingState(nodeOffset, *listHandle, transaction->getType());
        lists->readValues(adjNodeIDVector, *listHandle);
        relPropertyLists->readValues(relIDVector, *relListHandles);
        if (addToNextFrontier(nodeOffset, destNodeOffset)) {
            return true;
        }
        while (getNextBatchOfChildNodes()) {
            relPropertyLists->readValues(relIDVector, *relListHandles);
            if (addToNextFrontier(nodeOffset, destNodeOffset)) {
                return true;
            }
        }
    }
    return false;
}

bool ShortestPathAdjList::addToNextFrontier(
    node_offset_t parentNodeOffset, node_offset_t destNodeOffset) {
    node_offset_t nodeOffset;
    uint64_t relOffset;
    for (auto pos = 0u; pos < adjNodeIDVector->state->selVector->selectedSize; pos++) {
        auto offsetPos = adjNodeIDVector->state->selVector->selectedPositions[pos];
        nodeOffset = adjNodeIDVector->readNodeOffset(offsetPos);
        if (bfsVisitedNodesMap.contains(nodeOffset)) {
            continue;
        }
        relOffset = relIDVector->getValue<int64_t>(offsetPos);
        auto nodeState = make_unique<NodeState>(parentNodeOffset, relOffset);
        bfsVisitedNodesMap[nodeOffset] = move(nodeState);
        if (nodeOffset == destNodeOffset) {
            return true;
        }
        nextFrontier->push_back(nodeOffset);
    }
    return false;
}

bool ShortestPathAdjList::getNextBatchOfChildNodes() {
    if (listHandle->listSyncState.hasMoreToRead()) {
        lists->readValues(adjNodeIDVector, *listHandle);
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
            cout << "<" << bfsVisitedNodesMap[destNodeOffset]->relParentID << "> ";
        }
        destNodeOffset = bfsVisitedNodesMap[destNodeOffset]->parentNodeID;
    } while (destNodeOffset != UINT64_MAX);
    cout << endl;
}

} // namespace processor
} // namespace kuzu
