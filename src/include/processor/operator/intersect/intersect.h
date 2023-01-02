#pragma once

#include "processor/operator/intersect/intersect_build.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct IntersectDataInfo {
    DataPos keyDataPos;
    vector<DataPos> payloadsDataPos;
};

class Intersect : public PhysicalOperator {
public:
    Intersect(const DataPos& outputDataPos, vector<IntersectDataInfo> intersectDataInfos,
        vector<shared_ptr<IntersectSharedState>> sharedHTs,
        vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INTERSECT, std::move(children), id, paramsString},
          outputDataPos{outputDataPos},
          intersectDataInfos{std::move(intersectDataInfos)}, sharedHTs{std::move(sharedHTs)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<PhysicalOperator>> clonedChildren;
        clonedChildren.push_back(children[0]->clone());
        return make_unique<Intersect>(outputDataPos, intersectDataInfos, sharedHTs,
            std::move(clonedChildren), id, paramsString);
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
