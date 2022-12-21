#pragma once

#include "queue"

#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

struct NodeState {
    uint64_t parentNodeID;
    uint64_t relParentID;
    shared_ptr<ValueVector> children;
    shared_ptr<ValueVector> relIDVector;

    NodeState(uint64_t parentNodeID, uint64_t relParentID, shared_ptr<ValueVector> children,
        shared_ptr<ValueVector> relIDVector)
        : parentNodeID{parentNodeID}, relParentID{relParentID}, children{move(children)},
          relIDVector{move(relIDVector)} {}
};

class BaseShortestPath : public PhysicalOperator {
public:
    BaseShortestPath(const DataPos& srcDataPos, const DataPos& destDataPos, uint64_t lowerBound,
        uint64_t upperBound, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, srcDataPos{srcDataPos},
          destDataPos{destDataPos}, lowerBound{lowerBound}, upperBound{upperBound},
          currFrontier{make_shared<vector<node_offset_t>>()},
          nextFrontier{make_shared<vector<node_offset_t>>()},
          listSyncState{make_shared<ListSyncState>()}, listHandle{make_shared<ListHandle>(
                                                           *listSyncState)},
          bfsVisitedNodesMap{map<uint64_t, shared_ptr<NodeState>>()} {}

    virtual bool getNextTuplesInternal() = 0;

    shared_ptr<ResultSet> init(ExecutionContext* context);

    virtual unique_ptr<PhysicalOperator> clone() = 0;

protected:
    DataPos srcDataPos;
    DataPos destDataPos;
    uint64_t lowerBound;
    uint64_t upperBound;
    shared_ptr<ValueVector> srcValueVector;
    shared_ptr<ValueVector> destValueVector;
    map<uint64_t, shared_ptr<NodeState>> bfsVisitedNodesMap;
    shared_ptr<vector<node_offset_t>> currFrontier;
    shared_ptr<vector<node_offset_t>> nextFrontier;
    shared_ptr<ListSyncState> listSyncState;
    shared_ptr<ListHandle> listHandle;
    MemoryManager* memoryManager;
};

} // namespace processor
} // namespace kuzu
