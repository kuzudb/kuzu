#pragma once

#include "recursive_join.h"

namespace kuzu {
namespace processor {

class ShortestPathRecursiveJoin : public BaseRecursiveJoin {
public:
    ShortestPathRecursiveJoin(uint8_t lowerBound, uint8_t upperBound, storage::NodeTable* nodeTable,
        std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::vector<DataPos> vectorsToScanPos, std::vector<ft_col_idx_t> colIndicesToScan,
        const DataPos& srcNodeIDVectorPos, const DataPos& dstNodeIDVectorPos,
        const DataPos& distanceVectorPos, const DataPos& tmpDstNodeIDVectorPos,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString,
        std::unique_ptr<PhysicalOperator> recursiveRoot)
        : BaseRecursiveJoin{lowerBound, upperBound, nodeTable, std::move(sharedState),
              std::move(vectorsToScanPos), std::move(colIndicesToScan), srcNodeIDVectorPos,
              dstNodeIDVectorPos, tmpDstNodeIDVectorPos, std::move(child), id, paramsString,
              std::move(recursiveRoot)},
          distanceVectorPos{distanceVectorPos} {}

    ShortestPathRecursiveJoin(uint8_t lowerBound, uint8_t upperBound, storage::NodeTable* nodeTable,
        std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::vector<DataPos> vectorsToScanPos, std::vector<ft_col_idx_t> colIndicesToScan,
        const DataPos& srcNodeIDVectorPos, const DataPos& dstNodeIDVectorPos,
        const DataPos& distanceVectorPos, const DataPos& tmpDstNodeIDVectorPos, uint32_t id,
        const std::string& paramsString, std::unique_ptr<PhysicalOperator> recursiveRoot)
        : BaseRecursiveJoin{lowerBound, upperBound, nodeTable, std::move(sharedState),
              std::move(vectorsToScanPos), std::move(colIndicesToScan), srcNodeIDVectorPos,
              dstNodeIDVectorPos, tmpDstNodeIDVectorPos, id, paramsString,
              std::move(recursiveRoot)},
          distanceVectorPos{distanceVectorPos} {}

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ShortestPathRecursiveJoin>(lowerBound, upperBound, nodeTable,
            sharedState, vectorsToScanPos, colIndicesToScan, srcNodeIDVectorPos, dstNodeIDVectorPos,
            distanceVectorPos, tmpDstNodeIDVectorPos, id, paramsString, recursiveRoot->clone());
    }

private:
    bool scanOutput() override;

private:
    DataPos distanceVectorPos;

    std::shared_ptr<common::ValueVector> distanceVector;
};

} // namespace processor
} // namespace kuzu
