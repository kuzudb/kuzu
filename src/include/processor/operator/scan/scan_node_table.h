#pragma once

#include "processor/operator/scan/scan_table.h"
#include "storage/predicate/column_predicate.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

struct ScanNodeTableProgressSharedState {
    common::node_group_idx_t numGroupsScanned;
    common::node_group_idx_t numGroups;

    ScanNodeTableProgressSharedState() : numGroupsScanned{0}, numGroups{0} {};
};

class ScanNodeTableSharedState {
public:
    explicit ScanNodeTableSharedState(std::unique_ptr<common::RoaringBitmapSemiMask> semiMask)
        : table{nullptr}, currentCommittedGroupIdx{common::INVALID_NODE_GROUP_IDX},
          currentUnCommittedGroupIdx{common::INVALID_NODE_GROUP_IDX}, numCommittedNodeGroups{0},
          numUnCommittedNodeGroups{0}, semiMask{std::move(semiMask)} {};

    void initialize(const transaction::Transaction* transaction, storage::NodeTable* table,
        ScanNodeTableProgressSharedState& progressSharedState);

    void nextMorsel(storage::NodeTableScanState& scanState,
        ScanNodeTableProgressSharedState& progressSharedState);

    common::RoaringBitmapSemiMask* getSemiMask() const { return semiMask.get(); }

private:
    std::mutex mtx;
    storage::NodeTable* table;
    common::node_group_idx_t currentCommittedGroupIdx;
    common::node_group_idx_t currentUnCommittedGroupIdx;
    common::node_group_idx_t numCommittedNodeGroups;
    common::node_group_idx_t numUnCommittedNodeGroups;
    std::unique_ptr<common::RoaringBitmapSemiMask> semiMask;
};

struct ScanNodeTableInfo {
    storage::NodeTable* table;
    std::vector<common::column_id_t> columnIDs;
    std::vector<storage::ColumnPredicateSet> columnPredicates;

    std::unique_ptr<storage::NodeTableScanState> localScanState = nullptr;

    ScanNodeTableInfo(storage::NodeTable* table, std::vector<common::column_id_t> columnIDs,
        std::vector<storage::ColumnPredicateSet> columnPredicates)
        : table{table}, columnIDs{std::move(columnIDs)},
          columnPredicates{std::move(columnPredicates)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ScanNodeTableInfo);

    void initScanState(common::RoaringBitmapSemiMask* semiMask);

private:
    ScanNodeTableInfo(const ScanNodeTableInfo& other)
        : table{other.table}, columnIDs{other.columnIDs},
          columnPredicates{copyVector(other.columnPredicates)} {}
};

struct ScanNodeTablePrintInfo final : OPPrintInfo {
    std::vector<std::string> tableNames;
    std::string alias;
    binder::expression_vector properties;

    ScanNodeTablePrintInfo(std::vector<std::string> tableNames, std::string alias,
        binder::expression_vector properties)
        : tableNames{std::move(tableNames)}, alias{std::move(alias)},
          properties{std::move(properties)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<ScanNodeTablePrintInfo>(new ScanNodeTablePrintInfo(*this));
    }

private:
    ScanNodeTablePrintInfo(const ScanNodeTablePrintInfo& other)
        : OPPrintInfo{other}, tableNames{other.tableNames}, alias{other.alias},
          properties{other.properties} {}
};

class ScanNodeTable final : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SCAN_NODE_TABLE;

public:
    ScanNodeTable(ScanTableInfo info, std::vector<ScanNodeTableInfo> nodeInfos,
        std::vector<std::shared_ptr<ScanNodeTableSharedState>> sharedStates, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo,
        std::shared_ptr<ScanNodeTableProgressSharedState> progressSharedState)
        : ScanTable{type_, std::move(info), id, std::move(printInfo)}, currentTableIdx{0},
          nodeInfos{std::move(nodeInfos)}, sharedStates{std::move(sharedStates)},
          progressSharedState{std::move(progressSharedState)} {
        KU_ASSERT(this->nodeInfos.size() == this->sharedStates.size());
    }

    std::vector<common::RoaringBitmapSemiMask*> getSemiMasks() const;

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    const ScanNodeTableSharedState& getSharedState(common::idx_t idx) const {
        KU_ASSERT(idx < sharedStates.size());
        return *sharedStates[idx];
    }

    std::unique_ptr<PhysicalOperator> clone() override;

    double getProgress(ExecutionContext* context) const override;

private:
    void initGlobalStateInternal(ExecutionContext* context) override;
    void initVectors(storage::TableScanState& state, const ResultSet& resultSet) const override;

private:
    common::idx_t currentTableIdx;
    std::vector<ScanNodeTableInfo> nodeInfos;
    std::vector<std::shared_ptr<ScanNodeTableSharedState>> sharedStates;
    std::shared_ptr<ScanNodeTableProgressSharedState> progressSharedState;
};

} // namespace processor
} // namespace kuzu
