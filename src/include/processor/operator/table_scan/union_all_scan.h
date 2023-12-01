#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct UnionAllScanInfo {
    std::vector<DataPos> outputPositions;
    std::vector<ft_col_idx_t> columnIndices;

    UnionAllScanInfo(std::vector<DataPos> outputPositions, std::vector<ft_col_idx_t> columnIndices)
        : outputPositions{std::move(outputPositions)}, columnIndices{std::move(columnIndices)} {}
    UnionAllScanInfo(const UnionAllScanInfo& other)
        : outputPositions{other.outputPositions}, columnIndices{other.columnIndices} {}

    inline std::unique_ptr<UnionAllScanInfo> copy() const {
        return std::make_unique<UnionAllScanInfo>(*this);
    }
};

struct UnionAllScanMorsel {
    FactorizedTable* table;
    uint64_t startTupleIdx;
    uint64_t numTuples;

    UnionAllScanMorsel(FactorizedTable* table, uint64_t startTupleIdx, uint64_t numTuples)
        : table{table}, startTupleIdx{startTupleIdx}, numTuples{numTuples} {}
};

class UnionAllScanSharedState {
public:
    UnionAllScanSharedState(
        std::vector<std::shared_ptr<FactorizedTable>> tables, uint64_t maxMorselSize)
        : tables{std::move(tables)}, maxMorselSize{maxMorselSize}, tableIdx{0}, nextTupleIdxToScan{
                                                                                    0} {}

    std::unique_ptr<UnionAllScanMorsel> getMorsel();

private:
    std::unique_ptr<UnionAllScanMorsel> getMorselNoLock(FactorizedTable* table);

private:
    std::mutex mtx;
    std::vector<std::shared_ptr<FactorizedTable>> tables;
    uint64_t maxMorselSize;
    uint64_t tableIdx;
    uint64_t nextTupleIdxToScan;
};

class UnionAllScan : public PhysicalOperator {
public:
    UnionAllScan(std::unique_ptr<UnionAllScanInfo> info,
        std::shared_ptr<UnionAllScanSharedState> sharedState,
        std::vector<std::unique_ptr<PhysicalOperator>> children, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::UNION_ALL_SCAN, std::move(children), id,
              paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    // For clone only
    UnionAllScan(std::unique_ptr<UnionAllScanInfo> info,
        std::shared_ptr<UnionAllScanSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::UNION_ALL_SCAN, id, paramsString},
          info{std::move(info)}, sharedState{std::move(sharedState)} {}

    bool isSource() const final { return true; }

    void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<UnionAllScan>(info->copy(), sharedState, id, paramsString);
    }

private:
    std::unique_ptr<UnionAllScanInfo> info;
    std::shared_ptr<UnionAllScanSharedState> sharedState;
    std::vector<common::ValueVector*> vectors;
};

} // namespace processor
} // namespace kuzu
