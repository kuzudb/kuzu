#pragma once

#include "processor/operator/order_by/sort_state.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct OrderByScanLocalState {
    std::vector<common::ValueVector*> vectorsToRead;
    std::unique_ptr<PayloadScanner> payloadScanner;

    void init(
        std::vector<DataPos>& outVectorPos, SortSharedState& sharedState, ResultSet& resultSet);

    inline uint64_t scan() { return payloadScanner->scan(vectorsToRead); }
};

// To preserve the ordering of tuples, the orderByScan operator will only
// be executed in single-thread mode.
class OrderByScan : public PhysicalOperator {
public:
    OrderByScan(std::vector<DataPos> outVectorPos, std::shared_ptr<SortSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::ORDER_BY_SCAN, std::move(child), id, paramsString},
          outVectorPos{std::move(outVectorPos)},
          localState{std::make_unique<OrderByScanLocalState>()}, sharedState{
                                                                     std::move(sharedState)} {}

    // This constructor is used for cloning only.
    OrderByScan(std::vector<DataPos> outVectorPos, std::shared_ptr<SortSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::ORDER_BY_SCAN, id, paramsString},
          outVectorPos{std::move(outVectorPos)},
          localState{std::make_unique<OrderByScanLocalState>()}, sharedState{
                                                                     std::move(sharedState)} {}

    inline bool isSource() const final { return true; }
    // Ordered table should be scanned in single-thread mode.
    inline bool canParallel() const final { return false; }

    bool getNextTuplesInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<OrderByScan>(outVectorPos, sharedState, id, paramsString);
    }

private:
    std::vector<DataPos> outVectorPos;
    std::unique_ptr<OrderByScanLocalState> localState;
    std::shared_ptr<SortSharedState> sharedState;
};

} // namespace processor
} // namespace kuzu
