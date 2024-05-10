#pragma once

#include "processor/operator/scan/scan_table.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

// class ScanNodeTableSharedState {
// public:
//     ScanNodeTableSharedState()
//         : table{nullptr}, currentGroupIdx{common::INVALID_NODE_GROUP_IDX}, numNodeGroups{0} {};
//
//     void initialize(const transaction::Transaction* transaction, storage::NodeTable* table);
//
//     common::node_group_idx_t getNextMorsel();
//
// private:
//     std::mutex mtx;
//     storage::NodeTable* table;
//     common::node_group_idx_t currentGroupIdx;
//     // TODO: Should we differentiate local, persistent and delta node groups?
//     common::node_group_idx_t numNodeGroups;
// };
//
// struct ScanNodeTableInfo {
//     storage::NodeTable* table;
//     std::vector<common::column_id_t> columnIDs;
//
//     ScanNodeTableInfo(storage::NodeTable* table, std::vector<common::column_id_t> columnIDs)
//         : table{table}, columnIDs{std::move(columnIDs)} {}
//     ScanNodeTableInfo(const ScanNodeTableInfo& other)
//         : table{other.table}, columnIDs{other.columnIDs} {}
//
//     std::unique_ptr<ScanNodeTableInfo> copy() const {
//         return std::make_unique<ScanNodeTableInfo>(*this);
//     }
// };

// class ScanNodeTable final : public ScanTable {
// public:
//     ScanNodeTable(std::unique_ptr<ScanNodeTableInfo> info, const DataPos& inVectorPos,
//         std::vector<DataPos> outVectorsPos, std::shared_ptr<ScanNodeTableSharedState>
//         sharedState, uint32_t id, const std::string& paramsString) :
//         ScanNodeTable{PhysicalOperatorType::SCAN_NODE_TABLE, std::move(info), inVectorPos,
//               std::move(outVectorsPos), std::move(sharedState), id, paramsString} {}
//
//     bool isSource() const override { return true; }
//
//     void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;
//
//     bool getNextTuplesInternal(ExecutionContext* context) override;
//
//     std::unique_ptr<PhysicalOperator> clone() override {
//         return make_unique<ScanNodeTable>(info->copy(), nodeIDPos, outVectorsPos, sharedState,
//         id,
//             paramsString);
//     }

// protected:
//     ScanNodeTable(PhysicalOperatorType operatorType, std::unique_ptr<ScanNodeTableInfo> info,
//         const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
//         std::shared_ptr<ScanNodeTableSharedState> sharedState, uint32_t id,
//         const std::string& paramsString)
//         : ScanTable{operatorType, inVectorPos, std::move(outVectorsPos), id, paramsString},
//           info{std::move(info)}, sharedState{std::move(sharedState)} {}
//
// private:
//     void initGlobalStateInternal(ExecutionContext* context) override {
//         sharedState->initialize(context->clientContext->getTx(), info->table);
//     }
//
// private:
//     std::unique_ptr<ScanNodeTableInfo> info;
//     std::shared_ptr<ScanNodeTableSharedState> sharedState;
//     std::unique_ptr<storage::NodeTableReadState> readState;
// };

} // namespace processor
} // namespace kuzu
