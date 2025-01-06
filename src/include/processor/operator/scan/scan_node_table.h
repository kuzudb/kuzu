#pragma once

#include "processor/operator/scan/scan_table.h"
#include "storage/predicate/column_predicate.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class ScanNodeTableSharedState {
public:
    explicit ScanNodeTableSharedState(std::unique_ptr<NodeVectorLevelSemiMask> semiMask,
                                      common::node_group_idx_t startNodeGroupIdx = 0,
                                      common::node_group_idx_t endNodeGroupIdx = common::INVALID_NODE_GROUP_IDX)
            : table{nullptr}, currentCommittedGroupIdx{common::INVALID_NODE_GROUP_IDX},
              currentUnCommittedGroupIdx{common::INVALID_NODE_GROUP_IDX}, numCommittedNodeGroups{0},
              semiMask{std::move(semiMask)}, startNodeGroupIdx(startNodeGroupIdx), endNodeGroupIdx(endNodeGroupIdx) {};

    void initialize(transaction::Transaction* transaction, storage::NodeTable* table);

    void reset(transaction::Transaction* transaction, storage::NodeTable* table);

    void markNodeGroupAsFinished(storage::NodeTableScanState& scanState);

    void updateVectorIdx(storage::NodeTableScanState& scanState);

    void nextMorsel(storage::NodeTableScanState& scanState);

    NodeSemiMask* getSemiMask() const { return semiMask.get(); }

private:
    std::mutex mtx;
    storage::NodeTable* table;
    common::node_group_idx_t currentCommittedGroupIdx;
    std::vector<common::idx_t> committedNodeGroupVectorIdx;
    std::vector<bool> committedNodeGroupFinished;
    common::node_group_idx_t currentUnCommittedGroupIdx;
    common::node_group_idx_t numCommittedNodeGroups;
    std::vector<storage::LocalNodeGroup*> localNodeGroups;
    std::unique_ptr<NodeVectorLevelSemiMask> semiMask;
    common::node_group_idx_t startNodeGroupIdx;
    common::node_group_idx_t endNodeGroupIdx;
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

    void reset(transaction::Transaction* transaction);

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
