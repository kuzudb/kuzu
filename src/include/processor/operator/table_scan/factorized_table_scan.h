#pragma once

#include "processor/data_pos.h"
#include "processor/operator/physical_operator.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct FactorizedTableScanInfo {
    std::vector<DataPos> outputPositions;
    std::vector<ft_col_idx_t> columnIndices;

    FactorizedTableScanInfo(
        std::vector<DataPos> outputPositions, std::vector<ft_col_idx_t> columnIndices)
        : outputPositions{std::move(outputPositions)}, columnIndices{std::move(columnIndices)} {}
    FactorizedTableScanInfo(const FactorizedTableScanInfo& other)
        : outputPositions{other.outputPositions}, columnIndices{other.columnIndices} {}

    inline std::unique_ptr<FactorizedTableScanInfo> copy() const {
        return std::make_unique<FactorizedTableScanInfo>(*this);
    }
};

class FactorizedTableScan;

struct FactorizedTableScanMorsel {
    uint64_t startTupleIdx;
    uint64_t numTuples;

    FactorizedTableScanMorsel(uint64_t startTupleIdx, uint64_t numTuples)
        : startTupleIdx{startTupleIdx}, numTuples{numTuples} {}
};

class FactorizedTableScanSharedState {
    friend class FactorizedTableScan;

public:
    FactorizedTableScanSharedState(std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize)
        : table{std::move(table)}, maxMorselSize{maxMorselSize}, nextTupleIdxToScan{0} {}

    std::unique_ptr<FactorizedTableScanMorsel> getMorsel();

private:
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> table;
    uint64_t maxMorselSize;
    uint64_t nextTupleIdxToScan;
};

class FactorizedTableScan : public PhysicalOperator {
public:
    FactorizedTableScan(std::unique_ptr<FactorizedTableScanInfo> info,
        std::shared_ptr<FactorizedTableScanSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::FACTORIZED_TABLE_SCAN, std::move(child), id,
              paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    FactorizedTableScan(std::unique_ptr<FactorizedTableScanInfo> info,
        std::shared_ptr<FactorizedTableScanSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::FACTORIZED_TABLE_SCAN, id, paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    bool isSource() const final { return true; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return make_unique<FactorizedTableScan>(info->copy(), sharedState, id, paramsString);
    }

private:
    std::unique_ptr<FactorizedTableScanInfo> info;
    std::shared_ptr<FactorizedTableScanSharedState> sharedState;
    std::vector<common::ValueVector*> vectors;
};

} // namespace processor
} // namespace kuzu
