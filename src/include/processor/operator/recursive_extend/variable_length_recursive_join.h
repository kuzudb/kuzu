#pragma once

#include "recursive_join.h"

namespace kuzu {
namespace processor {

class VariableLengthRecursiveJoin : public BaseRecursiveJoin {
public:
    VariableLengthRecursiveJoin(uint8_t lowerBound, uint8_t upperBound,
        storage::NodeTable* nodeTable, std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::vector<DataPos> vectorsToScanPos, std::vector<ft_col_idx_t> colIndicesToScan,
        const DataPos& srcNodeIDVectorPos, const DataPos& dstNodeIDVectorPos,
        const DataPos& tmpDstNodeIDVectorPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString, std::unique_ptr<PhysicalOperator> recursiveRoot)
        : BaseRecursiveJoin{lowerBound, upperBound, nodeTable, std::move(sharedState),
              std::move(vectorsToScanPos), std::move(colIndicesToScan), srcNodeIDVectorPos,
              dstNodeIDVectorPos, tmpDstNodeIDVectorPos, std::move(child), id, paramsString,
              std::move(recursiveRoot)} {}

    VariableLengthRecursiveJoin(uint8_t lowerBound, uint8_t upperBound,
        storage::NodeTable* nodeTable, std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::vector<DataPos> vectorsToScanPos, std::vector<ft_col_idx_t> colIndicesToScan,
        const DataPos& srcNodeIDVectorPos, const DataPos& dstNodeIDVectorPos,
        const DataPos& tmpDstNodeIDVectorPos, uint32_t id, const std::string& paramsString,
        std::unique_ptr<PhysicalOperator> recursiveRoot)
        : BaseRecursiveJoin{lowerBound, upperBound, nodeTable, std::move(sharedState),
              std::move(vectorsToScanPos), std::move(colIndicesToScan), srcNodeIDVectorPos,
              dstNodeIDVectorPos, tmpDstNodeIDVectorPos, id, paramsString,
              std::move(recursiveRoot)} {}

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<VariableLengthRecursiveJoin>(lowerBound, upperBound, nodeTable,
            sharedState, vectorsToScanPos, colIndicesToScan, srcNodeIDVectorPos, dstNodeIDVectorPos,
            tmpDstNodeIDVectorPos, id, paramsString, recursiveRoot->clone());
    }

private:
    bool scanOutput() override;
};

} // namespace processor
} // namespace kuzu
