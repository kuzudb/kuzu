#pragma once

#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class IntersectProbe {
public:
    IntersectProbe(shared_ptr<HashJoinSharedState> joinSharedState, DataPos probeKeyVectorPos,
                   unique_ptr<IntersectProbe> childProbe, PhysicalOperator* nextOp)
        : probeKeyVectorPos{probeKeyVectorPos}, probeKeyVector{nullptr},
          joinSharedState{move(joinSharedState)}, prevProbeKey{UINT64_MAX, UINT64_MAX},
          isCachingAvailable{false}, childProbe{move(childProbe)}, nextOp{nextOp} {
        probeResult = make_shared<ValueVector>(NODE_ID);
        probeResult->state = make_shared<DataChunkState>();
        probeState = make_unique<ProbeState>();
    }

    void init(ResultSet& resultSet) {
        if (childProbe) {
            childProbe->init(resultSet);
        }
        probeKeyVector = resultSet.getValueVector(probeKeyVectorPos);
    }

    bool probe();

private:
    bool getChildNextTuples();
    void fetchFromHT();
    void probeHT(const nodeID_t& probeKey);
    void setPrevKeyForCacheIfNecessary(const nodeID_t& probeKey);

public:
    DataPos probeKeyVectorPos;
    shared_ptr<ValueVector> probeKeyVector;
    shared_ptr<ValueVector> probeResult;
    unique_ptr<ProbeState> probeState;
    shared_ptr<HashJoinSharedState> joinSharedState;
    nodeID_t prevProbeKey;
    bool isCachingAvailable;
    unique_ptr<IntersectProbe> childProbe;
    PhysicalOperator* nextOp;
};

class Intersect : public PhysicalOperator {

public:
    Intersect(const DataPos& outputVectorPos, vector<DataPos> probeSideKeyVectorsPos,
              vector<shared_ptr<HashJoinSharedState>> sharedStates,
              vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(children), id, paramsString}, outputVectorPos{outputVectorPos},
          probeSideKeyVectorsPos{move(probeSideKeyVectorsPos)}, sharedStates{move(sharedStates)} {
        // For regular join without sip, this should always be true.
        assert(this->sharedStates.size() == this->children.size() - 1);
        probeResults.resize(this->sharedStates.size());
        prober = createIntersectProber(0);
    }

    inline unique_ptr<IntersectProbe> createIntersectProber(uint32_t branchIdx) {
        auto childProbe = branchIdx == this->sharedStates.size() - 1 ?
                          nullptr :
                          createIntersectProber(branchIdx + 1);
        auto intersectProbe = make_unique<IntersectProbe>(this->sharedStates[branchIdx],
                                                          probeSideKeyVectorsPos[branchIdx], move(childProbe), children[0].get());
        probeResults[branchIdx] = intersectProbe->probeResult;
        return intersectProbe;
    }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;
    bool getNextTuples() override;

    inline PhysicalOperatorType getOperatorType() override { return MW_INTERSECT; }
    inline unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<PhysicalOperator>> clonedChildren;
        for (auto& child : children) {
            clonedChildren.push_back(child->clone());
        }
        return make_unique<Intersect>(outputVectorPos, probeSideKeyVectorsPos, sharedStates,
                                      move(clonedChildren), id, paramsString);
    }

private:
    static bool isCurrentIntersectDone(
        const vector<shared_ptr<ValueVector>>& vectors, const vector<sel_t>& positions);
    static nodeID_t getMaxNodeOffset(
        const vector<shared_ptr<ValueVector>>& vectors, const vector<sel_t>& positions);

    void kWayIntersect();

private:
    DataPos outputVectorPos;
    vector<DataPos> probeSideKeyVectorsPos;
    shared_ptr<ValueVector> outputVector;
    unique_ptr<IntersectProbe> prober;
    vector<shared_ptr<HashJoinSharedState>> sharedStates;
    vector<shared_ptr<ValueVector>> probeResults;
};

} // namespace processor
} // namespace graphflow