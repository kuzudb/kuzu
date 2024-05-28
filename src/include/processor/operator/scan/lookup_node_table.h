#pragma once

#include "processor/operator/scan/scan_node_table.h"

namespace kuzu {
namespace processor {

class LookupNodeTable : public ScanTable {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::LOOK_UP_NODE_TABLE;

public:
    LookupNodeTable(ScanTableInfo info, common::table_id_map_t<ScanNodeTableInfo> tableIDToNodeInfo,
        uint32_t id, const std::string& paramString)
        : ScanTable{type_, std::move(info), id, paramString},
          tableIDToNodeInfo{std::move(tableIDToNodeInfo)}, hasExecuted{false} {}

    void init(common::nodeID_t nodeID);

    bool isSource() const override { return true; }

    void initLocalStateInternal(ResultSet*, ExecutionContext*) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<LookupNodeTable>(info.copy(), copyMap(tableIDToNodeInfo), id,
            paramsString);
    }

private:
    common::table_id_map_t<ScanNodeTableInfo> tableIDToNodeInfo;
    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
