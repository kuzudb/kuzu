#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

class CrossProduct;
class CrossProductLocalState {
    friend class CrossProduct;

public:
    CrossProductLocalState(std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize)
        : table{std::move(table)}, maxMorselSize{maxMorselSize}, startIdx{0} {}

    void init() { startIdx = table->getNumTuples(); }

    inline std::unique_ptr<CrossProductLocalState> copy() const {
        return std::make_unique<CrossProductLocalState>(table, maxMorselSize);
    }

private:
    std::shared_ptr<FactorizedTable> table;
    uint64_t maxMorselSize;
    uint64_t startIdx = 0u;
};

class CrossProductInfo {
    friend class CrossProduct;

public:
    CrossProductInfo(std::vector<DataPos> outVecPos, std::vector<ft_col_idx_t> colIndicesToScan)
        : outVecPos{std::move(outVecPos)}, colIndicesToScan{std::move(colIndicesToScan)} {}
    CrossProductInfo(const CrossProductInfo& other)
        : outVecPos{other.outVecPos}, colIndicesToScan{other.colIndicesToScan} {}

    inline std::unique_ptr<CrossProductInfo> copy() const {
        return std::make_unique<CrossProductInfo>(*this);
    }

private:
    std::vector<DataPos> outVecPos;
    std::vector<ft_col_idx_t> colIndicesToScan;
};

class CrossProduct : public PhysicalOperator {
public:
    CrossProduct(std::unique_ptr<CrossProductInfo> info,
        std::unique_ptr<CrossProductLocalState> localState,
        std::unique_ptr<PhysicalOperator> probeChild, std::unique_ptr<PhysicalOperator> buildChild,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CROSS_PRODUCT, std::move(probeChild),
              std::move(buildChild), id, paramsString},
          info{std::move(info)}, localState{std::move(localState)} {}

    // Clone only.
    CrossProduct(std::unique_ptr<CrossProductInfo> info,
        std::unique_ptr<CrossProductLocalState> localState, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::CROSS_PRODUCT, std::move(child), id, paramsString},
          info{std::move(info)}, localState{std::move(localState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CrossProduct>(
            info->copy(), localState->copy(), children[0]->clone(), id, paramsString);
    }

private:
    std::unique_ptr<CrossProductInfo> info;
    std::unique_ptr<CrossProductLocalState> localState;
    std::vector<common::ValueVector*> vectorsToScan;
};

} // namespace processor
} // namespace kuzu
