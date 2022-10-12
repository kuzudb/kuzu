#pragma once

#include "src/processor/operator/include/physical_operator.h"
#include "src/processor/operator/intersect/include/intersect_build.h"

namespace graphflow {
namespace processor {

class Intersect : public PhysicalOperator {
public:
    Intersect(const DataPos& outputDataPos, vector<DataPos> probeKeysDataPos,
        vector<shared_ptr<IntersectSharedState>> sharedHTs,
        vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(children), id, paramsString}, outputDataPos{outputDataPos},
          probeKeysDataPos{move(probeKeysDataPos)}, sharedHTs{move(sharedHTs)} {}

    inline PhysicalOperatorType getOperatorType() override { return INTERSECT; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<PhysicalOperator>> clonedChildren;
        for (auto& child : children) {
            clonedChildren.push_back(child->clone());
        }
        return make_unique<Intersect>(
            outputDataPos, probeKeysDataPos, sharedHTs, move(clonedChildren), id, paramsString);
    }

private:
    vector<nodeID_t> getProbeKeys();
    vector<uint8_t*> probeHTs(const vector<nodeID_t>& keys);
    // Left is always the one with less num of values.
    static uint64_t twoWayIntersect(nodeID_t* leftNodeIDs, uint64_t leftSize,
        nodeID_t* rightNodeIDs, uint64_t rightSize, nodeID_t* outputNodeIDs);
    void intersectLists();

private:
    DataPos outputDataPos;
    vector<DataPos> probeKeysDataPos;
    shared_ptr<ValueVector> outputVector;
    vector<shared_ptr<ValueVector>> probeKeyVectors;
    vector<shared_ptr<IntersectSharedState>> sharedHTs;
    vector<bool> isIntersectListAFlatValue;
};

} // namespace processor
} // namespace graphflow
