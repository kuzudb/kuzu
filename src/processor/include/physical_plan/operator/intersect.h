#pragma once

#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class IntersectProbe {
public:
    IntersectProbe(shared_ptr<HashJoinSharedState> joinSharedState,
        const DataPos& probeKeyVectorPos, unique_ptr<IntersectProbe> childProbe,
        PhysicalOperator* nextOp)
        : probeKeyVectorPos{probeKeyVectorPos}, probeKeyVector{nullptr},
          joinSharedState{move(joinSharedState)}, prevProbeKey{UINT64_MAX, UINT64_MAX},
          childProbe{move(childProbe)}, nextOp{nextOp}, currentProbeKey{UINT64_MAX, UINT64_MAX} {
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
    bool getChildNextTuples() const;
    void fetchFromHT();
    void probeHT(const nodeID_t& probeKey) const;

public:
    DataPos probeKeyVectorPos;
    shared_ptr<ValueVector> probeKeyVector;
    unique_ptr<ProbeState> probeState;
    overflow_value_t probedResult;
    shared_ptr<HashJoinSharedState> joinSharedState;
    nodeID_t prevProbeKey;
    unique_ptr<IntersectProbe> childProbe;
    PhysicalOperator* nextOp;
    nodeID_t currentProbeKey;
};

class Intersect : public PhysicalOperator {

public:
    Intersect(const DataPos& outputVectorPos, vector<DataPos> probeSideKeyVectorsPos,
        vector<shared_ptr<HashJoinSharedState>> sharedStates,
        vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(children), id, paramsString}, outputVectorPos{outputVectorPos},
          probeSideKeyVectorsPos{move(probeSideKeyVectorsPos)}, sharedStates{move(sharedStates)} {
        probeResults.resize(this->sharedStates.size());
        probeStates.resize(this->sharedStates.size());
        currentProbeKeys.resize(this->sharedStates.size());
        prober = createIntersectProber(0);
        prefix = {UINT64_MAX, UINT64_MAX};
        cachedVector = make_unique<ValueVector>(NODE_ID);
        cachedState = make_shared<DataChunkState>();
        cachedVector->state = cachedState;
    }

    inline unique_ptr<IntersectProbe> createIntersectProber(uint32_t branchIdx) {
        auto childProbe = branchIdx == this->sharedStates.size() - 1 ?
                              nullptr :
                              createIntersectProber(branchIdx + 1);
        auto intersectProbe = make_unique<IntersectProbe>(this->sharedStates[branchIdx],
            probeSideKeyVectorsPos[branchIdx], move(childProbe), children[0].get());
        probeResults[branchIdx] = &intersectProbe->probedResult;
        probeStates[branchIdx] = intersectProbe->probeState.get();
        currentProbeKeys[branchIdx] = &intersectProbe->currentProbeKey;
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
    // Left is the smaller one.
    static uint64_t twoWayIntersect(uint8_t* leftValues, uint64_t leftSize, uint8_t* rightValues,
        uint64_t rightSize, uint8_t* output);
    void kWayIntersect();

private:
    DataPos outputVectorPos;
    vector<DataPos> probeSideKeyVectorsPos;
    shared_ptr<ValueVector> outputVector;
    unique_ptr<IntersectProbe> prober;
    vector<shared_ptr<HashJoinSharedState>> sharedStates;
    vector<overflow_value_t*> probeResults;
    vector<ProbeState*> probeStates;
    vector<nodeID_t*> currentProbeKeys;
    vector<node_offset_t> prefix;
    unique_ptr<ValueVector> cachedVector;
    shared_ptr<DataChunkState> cachedState;
};

} // namespace processor
} // namespace graphflow
