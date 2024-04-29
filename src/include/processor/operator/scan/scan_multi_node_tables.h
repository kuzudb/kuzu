#pragma once

#include "processor/operator/scan/scan_node_table.h"

namespace kuzu {
namespace processor {

class ScanMultiNodeTables : public ScanTable {
public:
    ScanMultiNodeTables(const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
        common::table_id_map_t<std::unique_ptr<ScanNodeTableInfo>> infos,
        std::unique_ptr<PhysicalOperator> prevOperator, uint32_t id,
        const std::string& paramsString)
        : ScanTable{PhysicalOperatorType::SCAN_MULTI_NODE_TABLES, inVectorPos,
              std::move(outVectorsPos), std::move(prevOperator), id, paramsString},
          infos{std::move(infos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override;

private:
    common::table_id_map_t<std::unique_ptr<ScanNodeTableInfo>> infos;
    common::table_id_map_t<std::unique_ptr<storage::NodeTableReadState>> readStates;
};

} // namespace processor
} // namespace kuzu
