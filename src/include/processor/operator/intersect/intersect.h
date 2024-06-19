#pragma once

#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct IntersectDataInfo {
    DataPos keyDataPos;
    // TODO(Xiyang): payload is not an accurate name for intersect.
    std::vector<DataPos> payloadsDataPos;
};

class Intersect : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::INTERSECT;

public:
    Intersect(const DataPos& outputDataPos, std::vector<IntersectDataInfo> intersectDataInfos,
        std::vector<std::shared_ptr<HashJoinSharedState>> sharedHTs,
        std::vector<std::unique_ptr<PhysicalOperator>> children, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(children), id, std::move(printInfo)},
          outputDataPos{outputDataPos}, intersectDataInfos{std::move(intersectDataInfos)},
          sharedHTs{std::move(sharedHTs)} {
        tupleIdxPerBuildSide.resize(this->sharedHTs.size(), 0);
        carryBuildSideIdx = -1u;
        probedFlatTuples.resize(this->sharedHTs.size());
    }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        std::vector<std::unique_ptr<PhysicalOperator>> clonedChildren;
        clonedChildren.push_back(children[0]->clone());
        return std::make_unique<Intersect>(outputDataPos, intersectDataInfos, sharedHTs,
            std::move(clonedChildren), id, printInfo->copy());
    }

private:
    // For each build side, probe its HT and return a vector of matched flat tuples.
    void probeHTs();
    // Left is always the one with less num of values.
    static void twoWayIntersect(common::nodeID_t* leftNodeIDs, common::SelectionVector& lSelVector,
        common::nodeID_t* rightNodeIDs, common::SelectionVector& rSelVector);
    void intersectLists(const std::vector<common::overflow_value_t>& listsToIntersect);
    void populatePayloads(const std::vector<uint8_t*>& tuples,
        const std::vector<uint32_t>& listIdxes);
    bool hasNextTuplesToIntersect();

    inline uint32_t getNumBuilds() { return sharedHTs.size(); }

private:
    DataPos outputDataPos;
    std::vector<IntersectDataInfo> intersectDataInfos;
    // payloadColumnIdxesToScanFrom and payloadVectorsToScanInto are organized by each build child.
    std::vector<std::vector<uint32_t>> payloadColumnIdxesToScanFrom;
    std::vector<std::vector<common::ValueVector*>> payloadVectorsToScanInto;
    std::shared_ptr<common::ValueVector> outKeyVector;
    std::vector<std::shared_ptr<common::ValueVector>> probeKeyVectors;
    std::vector<std::unique_ptr<common::SelectionVector>> intersectSelVectors;
    std::vector<std::shared_ptr<HashJoinSharedState>> sharedHTs;
    std::vector<bool> isIntersectListAFlatValue;
    std::vector<std::vector<uint8_t*>> probedFlatTuples;
    // Keep track of the tuple to intersect for each build side.
    std::vector<uint32_t> tupleIdxPerBuildSide;
    // This is used to indicate which build side to increment the tuple idx for.
    uint32_t carryBuildSideIdx;
};

} // namespace processor
} // namespace kuzu
