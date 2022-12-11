#pragma once

#include "queue"

#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

struct BFSState {
    uint64_t parentNodeID;
    uint64_t bfsLevel;
    shared_ptr<ValueVector> children;
    uint64_t currChildIdx;
    bool hasBeenOutput;
    shared_ptr<ListSyncState> listSyncState;
    shared_ptr<ListHandle> listHandle;

    BFSState(uint64_t parentNodeID, uint64_t bfsLevel, shared_ptr<ValueVector> children,
        uint64_t currChildIdx, bool hasBeenOutput, shared_ptr<ListSyncState> listSyncState,
        shared_ptr<ListHandle> listHandle)
        : parentNodeID{parentNodeID}, bfsLevel{bfsLevel}, children{move(children)},
          currChildIdx{currChildIdx}, hasBeenOutput{hasBeenOutput},
          listSyncState{move(listSyncState)}, listHandle{move(listHandle)} {}
};

class BaseShortestPath : public PhysicalOperator {
public:
    BaseShortestPath(const DataPos& srcDataPos, const DataPos& destDataPos,
        BaseColumnOrList* storage, uint64_t lowerBound, uint64_t upperBound,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, srcDataPos{srcDataPos},
          destDataPos{destDataPos}, storage{storage}, lowerBound{lowerBound},
          upperBound{upperBound}, bfsQueue{queue<uint64_t>()},
          bfsVisitedNodesMap{map<uint64_t, shared_ptr<BFSState>>()} {}

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
    map<uint64_t, shared_ptr<BFSState>> bfsVisitedNodesMap;
    queue<uint64_t> bfsQueue = queue<uint64_t>();
    BaseColumnOrList* storage;
    MemoryManager* memoryManager;
};

} // namespace processor
} // namespace kuzu
