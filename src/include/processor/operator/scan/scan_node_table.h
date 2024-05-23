#pragma once

#include "processor/operator/scan/scan_table.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class ScanNodeTableSharedState {
public:
    ScanNodeTableSharedState()
        : table{nullptr}, currentCommittedGroupIdx{common::INVALID_NODE_GROUP_IDX},
          currentUnCommittedGroupIdx{common::INVALID_NODE_GROUP_IDX}, numCommittedNodeGroups{0} {};

    void initialize(transaction::Transaction* transaction, storage::NodeTable* table);

    void nextMorsel(storage::NodeTableScanState& scanState);

private:
    std::mutex mtx;
    storage::NodeTable* table;
    common::node_group_idx_t currentCommittedGroupIdx;
    common::node_group_idx_t currentUnCommittedGroupIdx;
    common::node_group_idx_t numCommittedNodeGroups;
    std::vector<storage::LocalNodeGroup*> localNodeGroups;
};

struct ScanNodeTableInfo {
    storage::NodeTable* table;
    std::vector<common::column_id_t> columnIDs;

    ScanNodeTableInfo(storage::NodeTable* table, std::vector<common::column_id_t> columnIDs)
        : table{table}, columnIDs{std::move(columnIDs)} {}
    ScanNodeTableInfo(const ScanNodeTableInfo& other)
        : table{other.table}, columnIDs{other.columnIDs} {}

    std::unique_ptr<ScanNodeTableInfo> copy() const {
        return std::make_unique<ScanNodeTableInfo>(*this);
    }
};

class ScanNodeTable final : public ScanTable {
public:
    ScanNodeTable(const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
        std::vector<std::unique_ptr<ScanNodeTableInfo>> infos,
        std::vector<std::shared_ptr<ScanNodeTableSharedState>> sharedStates, uint32_t id,
        const std::string& paramsString)
        : ScanTable{PhysicalOperatorType::SCAN_NODE_TABLE, inVectorPos, std::move(outVectorsPos),
              id, paramsString},
          currentTableIdx{0}, infos{std::move(infos)}, sharedStates{std::move(sharedStates)} {
        KU_ASSERT(this->infos.size() == this->sharedStates.size());
    }

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    common::vector_idx_t getNumTables() const { return sharedStates.size(); }
    const ScanNodeTableSharedState& getSharedState(common::vector_idx_t idx) const {
        KU_ASSERT(idx < sharedStates.size());
        return *sharedStates[idx];
    }

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    void initGlobalStateInternal(ExecutionContext* context) override;

private:
    common::vector_idx_t currentTableIdx;
    // TODO(Guodong): Refactor following three fields into a vector of structs.
    std::vector<std::unique_ptr<ScanNodeTableInfo>> infos;
    std::vector<std::shared_ptr<ScanNodeTableSharedState>> sharedStates;
    std::vector<std::unique_ptr<storage::NodeTableScanState>> scanStates;
};

} // namespace processor
} // namespace kuzu
