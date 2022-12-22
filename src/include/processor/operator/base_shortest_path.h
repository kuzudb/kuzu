#pragma once

#include "queue"

#include "processor/operator/physical_operator.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

struct NodeState {
    uint64_t parentNodeID;
    uint64_t relParentID;

    NodeState(uint64_t parentNodeID, uint64_t relParentID)
        : parentNodeID{parentNodeID}, relParentID{relParentID} {}
};

class BaseShortestPath : public PhysicalOperator {
public:
    BaseShortestPath(PhysicalOperatorType physicalOperatorType, const DataPos& srcDataPos,
        const DataPos& destDataPos, uint64_t lowerBound, uint64_t upperBound,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : PhysicalOperator{physicalOperatorType, move(child), id, paramsString},
          srcDataPos{srcDataPos}, destDataPos{destDataPos}, lowerBound{lowerBound},
          upperBound{upperBound}, currFrontier{make_shared<vector<node_offset_t>>()},
          nextFrontier{make_shared<vector<node_offset_t>>()},
          bfsVisitedNodesMap{map<uint64_t, unique_ptr<NodeState>>()} {}

    virtual bool getNextTuplesInternal() = 0;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    virtual unique_ptr<PhysicalOperator> clone() = 0;

protected:
    DataPos srcDataPos;
    DataPos destDataPos;
    uint64_t lowerBound;
    uint64_t upperBound;
    shared_ptr<ValueVector> srcValueVector;
    shared_ptr<ValueVector> destValueVector;
    map<uint64_t, unique_ptr<NodeState>> bfsVisitedNodesMap;
    shared_ptr<vector<node_offset_t>> currFrontier;
    shared_ptr<vector<node_offset_t>> nextFrontier;
};

} // namespace processor
} // namespace kuzu
