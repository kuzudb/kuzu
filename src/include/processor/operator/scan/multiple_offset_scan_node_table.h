#pragma once

#include "processor/operator/scan/scan_node_table.h"

namespace kuzu {
namespace processor {

// OffsetScanNodeTable is only used as the source operator for RecursiveJoin and thus cannot be
// executed in parallel. Therefore, it does not have a shared state.
class MultipleOffsetScanNodeTable : public ScanTable {
    static constexpr PhysicalOperatorType type_ =
        PhysicalOperatorType::MULTIPLE_OFFSET_SCAN_NODE_TABLE;

public:
    MultipleOffsetScanNodeTable(ScanTableInfo info, std::unique_ptr<PhysicalOperator> child,
        ScanNodeTableInfo nodeTableInfo, uint32_t id, std::unique_ptr<OPPrintInfo> printInfo)
        : ScanTable{type_, std::move(info), std::move(child), id, std::move(printInfo)},
          nodeTableInfo{std::move(nodeTableInfo)} {}

    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<MultipleOffsetScanNodeTable>(info.copy(), children[0]->clone(), nodeTableInfo.copy(), id,
            printInfo->copy());
    }

private:
    ScanNodeTableInfo nodeTableInfo;
    std::unordered_map<common::node_group_idx_t, ScanNodeTableInfo> scanNodeInfoCache;
    std::unordered_map<common::node_group_idx_t, common::SelectionVector> selPerNG;
};

} // namespace processor
} // namespace kuzu
