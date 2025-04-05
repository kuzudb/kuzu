#pragma once

#include "processor/operator/scan/scan_table.h"
#include "storage/predicate/column_predicate.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

struct ScanRelTableProgressSharedState {
    common::node_group_idx_t numGroupsScanned;
    common::node_group_idx_t numGroups;

    ScanRelTableProgressSharedState() : numGroupsScanned{0}, numGroups{0} {};
};

class ScanRelTableSharedState {
public:
    explicit ScanRelTableSharedState(std::unique_ptr<common::SemiMask> semiMask)
        : table{nullptr}, currentCommittedGroupIdx{common::INVALID_NODE_GROUP_IDX},
          currentUnCommittedGroupIdx{common::INVALID_NODE_GROUP_IDX}, numCommittedNodeGroups{0},
          numUnCommittedNodeGroups{0}, semiMask{std::move(semiMask)} {};

    void initialize(const transaction::Transaction* transaction, storage::RelTable* table,
        ScanRelTableProgressSharedState& progressSharedState);

    void nextMorsel(storage::RelTableScanState& scanState,
        ScanRelTableProgressSharedState& progressSharedState);

    common::SemiMask* getSemiMask() const { return semiMask.get(); }

private:
    std::mutex mtx;
    storage::RelTable* table;
    common::node_group_idx_t currentCommittedGroupIdx;
    common::node_group_idx_t currentUnCommittedGroupIdx;
    common::node_group_idx_t numCommittedNodeGroups;
    common::node_group_idx_t numUnCommittedNodeGroups;
    std::unique_ptr<common::SemiMask> semiMask;
};

struct ScanRelTableInfo {
    storage::RelTable* table;
    std::vector<common::column_id_t> columnIDs;
    std::vector<storage::ColumnPredicateSet> columnPredicates;

    ScanRelTableInfo(storage::RelTable* table, std::vector<common::column_id_t> columnIDs,
        std::vector<storage::ColumnPredicateSet> columnPredicates)
        : table{table}, columnIDs{std::move(columnIDs)},
          columnPredicates{std::move(columnPredicates)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ScanRelTableInfo);

private:
    ScanRelTableInfo(const ScanRelTableInfo& other)
        : table{other.table}, columnIDs{other.columnIDs},
          columnPredicates{copyVector(other.columnPredicates)} {}
};

struct ScanRelTablePrintInfo final : OPPrintInfo {
    std::vector<std::string> tableNames;
    std::string alias;
    binder::expression_vector properties;

    ScanRelTablePrintInfo(std::vector<std::string> tableNames, std::string alias,
        binder::expression_vector properties)
        : tableNames{std::move(tableNames)}, alias{std::move(alias)},
          properties{std::move(properties)} {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<ScanRelTablePrintInfo>(new ScanRelTablePrintInfo(*this));
    }

private:
    ScanRelTablePrintInfo(const ScanRelTablePrintInfo& other)
        : OPPrintInfo{other}, tableNames{other.tableNames}, alias{other.alias},
          properties{other.properties} {}
};

class ScanRelTable final : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SCAN_REL_TABLE;

public:
    ScanRelTable(ScanTableInfo info, std::vector<ScanRelTableInfo> relInfos,
        std::vector<std::shared_ptr<ScanRelTableSharedState>> sharedStates, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo,
        std::shared_ptr<ScanRelTableProgressSharedState> progressSharedState)
        : ScanTable{type_, std::move(info), id, std::move(printInfo)}, currentTableIdx{0},
          scanState{nullptr}, relInfos{std::move(relInfos)}, sharedStates{std::move(sharedStates)},
          progressSharedState{std::move(progressSharedState)} {
        KU_ASSERT(this->relInfos.size() == this->sharedStates.size());
    }

    common::table_id_map_t<common::SemiMask*> getSemiMasks() const;

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    const ScanRelTableSharedState& getSharedState(common::idx_t idx) const {
        KU_ASSERT(idx < sharedStates.size());
        return *sharedStates[idx];
    }

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<ScanRelTable>(info.copy(), copyVector(relInfos), sharedStates, id,
            printInfo->copy(), progressSharedState);
    }

    double getProgress(ExecutionContext* context) const override;

private:
    void initGlobalStateInternal(ExecutionContext* context) override;

private:
    common::idx_t currentTableIdx;
    std::unique_ptr<storage::RelTableScanState> scanState;
    std::vector<ScanRelTableInfo> relInfos;
    std::vector<std::shared_ptr<ScanRelTableSharedState>> sharedStates;
    std::shared_ptr<ScanRelTableProgressSharedState> progressSharedState;
};

} // namespace processor
} // namespace kuzu
