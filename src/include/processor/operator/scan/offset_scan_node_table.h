#pragma once

#include "processor/operator/scan/scan_node_table.h"

namespace kuzu {
namespace processor {

// OffsetScanNodeTable is only used as the source operator for RecursiveJoin and thus cannot be
// executed in parallel. Therefore, it does not have a shared state.
class OffsetScanNodeTable : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::OFFSET_SCAN_NODE_TABLE;

public:
    OffsetScanNodeTable(ScanTableInfo info,
        common::table_id_map_t<ScanNodeTableInfo> tableIDToNodeInfo, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : ScanTable{type_, std::move(info), id, std::move(printInfo)},
          tableIDToNodeInfo{std::move(tableIDToNodeInfo)}, executed{false} {}

    void init(common::nodeID_t nodeID);

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<OffsetScanNodeTable>(info.copy(), copyMap(tableIDToNodeInfo), id,
            printInfo->copy());
    }

private:
    common::table_id_map_t<ScanNodeTableInfo> tableIDToNodeInfo;
    bool executed;
};

} // namespace processor
} // namespace kuzu
