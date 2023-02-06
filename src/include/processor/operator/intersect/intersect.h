#pragma once

#include "processor/operator/intersect/intersect_build.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct IntersectDataInfo {
    DataPos keyDataPos;
    std::vector<DataPos> payloadsDataPos;
};

class Intersect : public PhysicalOperator {
public:
    Intersect(const DataPos& outputDataPos, std::vector<IntersectDataInfo> intersectDataInfos,
        std::vector<std::shared_ptr<IntersectSharedState>> sharedHTs,
        std::vector<std::unique_ptr<PhysicalOperator>> children, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INTERSECT, std::move(children), id, paramsString},
          outputDataPos{outputDataPos},
          intersectDataInfos{std::move(intersectDataInfos)}, sharedHTs{std::move(sharedHTs)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        std::vector<std::unique_ptr<PhysicalOperator>> clonedChildren;
        clonedChildren.push_back(children[0]->clone());
        return std::make_unique<Intersect>(outputDataPos, intersectDataInfos, sharedHTs,
            std::move(clonedChildren), id, paramsString);
    }

private:
    std::vector<common::nodeID_t> getProbeKeys();
    std::vector<uint8_t*> probeHTs(const std::vector<common::nodeID_t>& keys);
    // Left is always the one with less num of values.
    static void twoWayIntersect(common::nodeID_t* leftNodeIDs, common::SelectionVector& lSelVector,
        common::nodeID_t* rightNodeIDs, common::SelectionVector& rSelVector);
    void intersectLists(const std::vector<common::overflow_value_t>& listsToIntersect);
    void populatePayloads(
        const std::vector<uint8_t*>& tuples, const std::vector<uint32_t>& listIdxes);

private:
    DataPos outputDataPos;
    std::vector<IntersectDataInfo> intersectDataInfos;
    // payloadColumnIdxesToScanFrom and payloadVectorsToScanInto are organized by each build child.
    std::vector<std::vector<uint32_t>> payloadColumnIdxesToScanFrom;
    std::vector<std::vector<std::shared_ptr<common::ValueVector>>> payloadVectorsToScanInto;
    std::shared_ptr<common::ValueVector> outKeyVector;
    std::vector<std::shared_ptr<common::ValueVector>> probeKeyVectors;
    std::vector<std::unique_ptr<common::SelectionVector>> intersectSelVectors;
    std::vector<std::shared_ptr<IntersectSharedState>> sharedHTs;
    std::vector<bool> isIntersectListAFlatValue;
};

} // namespace processor
} // namespace kuzu
