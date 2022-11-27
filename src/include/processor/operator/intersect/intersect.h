#pragma once

#include "processor/operator/intersect/intersect_build.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct IntersectDataInfo {
    DataPos keyDataPos;
    vector<DataPos> payloadsDataPos;
    vector<DataType> payloadsDataType;
};

class Intersect : public PhysicalOperator {
public:
    Intersect(const DataPos& outputDataPos, vector<IntersectDataInfo> intersectDataInfos,
        vector<shared_ptr<IntersectSharedState>> sharedHTs,
        vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(children), id, paramsString}, outputDataPos{outputDataPos},
          intersectDataInfos{move(intersectDataInfos)}, sharedHTs{move(sharedHTs)} {
        intersectSelVectors.resize(this->sharedHTs.size());
        for (auto i = 0u; i < this->sharedHTs.size(); i++) {
            intersectSelVectors[i] = make_unique<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
        }
    }

    inline PhysicalOperatorType getOperatorType() override { return INTERSECT; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<PhysicalOperator>> clonedChildren;
        for (auto& child : children) {
            clonedChildren.push_back(child->clone());
        }
        return make_unique<Intersect>(
            outputDataPos, intersectDataInfos, sharedHTs, move(clonedChildren), id, paramsString);
    }

private:
    vector<nodeID_t> getProbeKeys();
    vector<uint8_t*> probeHTs(const vector<nodeID_t>& keys);
    // Left is always the one with less num of values.
    static void twoWayIntersect(nodeID_t* leftNodeIDs, SelectionVector& lSelVector,
        nodeID_t* rightNodeIDs, SelectionVector& rSelVector);
    void intersectLists(const vector<overflow_value_t>& listsToIntersect);
    void populatePayloads(const vector<uint8_t*>& tuples, const vector<uint32_t>& listIdxes);

private:
    DataPos outputDataPos;
    vector<IntersectDataInfo> intersectDataInfos;
    // payloadColumnIdxesToScanFrom and payloadVectorsToScanInto are organized by each build child.
    vector<vector<uint32_t>> payloadColumnIdxesToScanFrom;
    vector<vector<shared_ptr<ValueVector>>> payloadVectorsToScanInto;
    shared_ptr<ValueVector> outKeyVector;
    vector<shared_ptr<ValueVector>> probeKeyVectors;
    vector<unique_ptr<SelectionVector>> intersectSelVectors;
    vector<shared_ptr<IntersectSharedState>> sharedHTs;
    vector<bool> isIntersectListAFlatValue;
};

} // namespace processor
} // namespace kuzu
