#pragma once

#include "processor/operator/scan/scan_table.h"
#include "storage/predicate/column_predicate.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class ScanNodeTableSharedState {
public:
    explicit ScanNodeTableSharedState(std::unique_ptr<NodeVectorLevelSemiMask> semiMask)
        : table{nullptr}, currentCommittedGroupIdx{common::INVALID_NODE_GROUP_IDX},
          currentUnCommittedGroupIdx{common::INVALID_NODE_GROUP_IDX}, numCommittedNodeGroups{0},
          semiMask{std::move(semiMask)} {};

    void initialize(transaction::Transaction* transaction, storage::NodeTable* table);

    void nextMorsel(storage::NodeTableScanState& scanState);

    NodeSemiMask* getSemiMask() const { return semiMask.get(); }

private:
    std::mutex mtx;
    storage::NodeTable* table;
    common::node_group_idx_t currentCommittedGroupIdx;
    common::node_group_idx_t currentUnCommittedGroupIdx;
    common::node_group_idx_t numCommittedNodeGroups;
    std::vector<storage::LocalNodeGroup*> localNodeGroups;
    std::unique_ptr<NodeVectorLevelSemiMask> semiMask;
};

struct ScanNodeTableInfo {
    storage::NodeTable* table;
    std::vector<common::column_id_t> columnIDs;
    std::vector<storage::ColumnPredicateSet> columnPredicates;

    std::unique_ptr<storage::NodeTableScanState> localScanState;

    ScanNodeTableInfo(storage::NodeTable* table, std::vector<common::column_id_t> columnIDs,
        std::vector<storage::ColumnPredicateSet> columnPredicates)
        : table{table}, columnIDs{std::move(columnIDs)},
          columnPredicates{std::move(columnPredicates)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(ScanNodeTableInfo);

    void initScanState(NodeSemiMask* semiMask);

private:
    ScanNodeTableInfo(const ScanNodeTableInfo& other)
        : table{other.table}, columnIDs{other.columnIDs},
          columnPredicates{copyVector(other.columnPredicates)} {}
};

class ScanNodeTable final : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SCAN_NODE_TABLE;

public:
    ScanNodeTable(ScanTableInfo info, std::vector<ScanNodeTableInfo> nodeInfos,
        std::vector<std::shared_ptr<ScanNodeTableSharedState>> sharedStates, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : ScanTable{type_, std::move(info), id, std::move(printInfo)}, currentTableIdx{0},
          nodeInfos{std::move(nodeInfos)}, sharedStates{std::move(sharedStates)} {
        KU_ASSERT(this->nodeInfos.size() == this->sharedStates.size());
    }

    std::vector<NodeSemiMask*> getSemiMasks();

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    const ScanNodeTableSharedState& getSharedState(common::idx_t idx) const {
        KU_ASSERT(idx < sharedStates.size());
        return *sharedStates[idx];
    }

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    void initGlobalStateInternal(ExecutionContext* context) override;

private:
    common::idx_t currentTableIdx;
    std::vector<ScanNodeTableInfo> nodeInfos;
    std::vector<std::shared_ptr<ScanNodeTableSharedState>> sharedStates;
};

} // namespace processor
} // namespace kuzu
