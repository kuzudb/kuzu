#pragma once

#include "recursive_join.h"

namespace kuzu {
namespace processor {

class ShortestPathRecursiveJoin : public BaseRecursiveJoin {
public:
    ShortestPathRecursiveJoin(uint8_t lowerBound, uint8_t upperBound,
        std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::unique_ptr<RecursiveJoinDataInfo> dataInfo, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString,
        std::unique_ptr<PhysicalOperator> recursiveRoot)
        : BaseRecursiveJoin{lowerBound, upperBound, std::move(sharedState), std::move(dataInfo),
              std::move(child), id, paramsString, std::move(recursiveRoot)} {}

    ShortestPathRecursiveJoin(uint8_t lowerBound, uint8_t upperBound,
        std::shared_ptr<RecursiveJoinSharedState> sharedState,
        std::unique_ptr<RecursiveJoinDataInfo> dataInfo, uint32_t id,
        const std::string& paramsString, std::unique_ptr<PhysicalOperator> recursiveRoot)
        : BaseRecursiveJoin{lowerBound, upperBound, std::move(sharedState), std::move(dataInfo), id,
              paramsString, std::move(recursiveRoot)} {}

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ShortestPathRecursiveJoin>(lowerBound, upperBound, sharedState,
            dataInfo->copy(), id, paramsString, recursiveRoot->clone());
    }
};

} // namespace processor
} // namespace kuzu
