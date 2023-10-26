#pragma once

#include "processor/operator/scan/scan_node_table.h"

namespace kuzu {
namespace processor {

class ScanMultiNodeTables : public ScanTable {
public:
    ScanMultiNodeTables(const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
        std::unordered_map<common::table_id_t, storage::NodeTable*> tables,
        std::unordered_map<common::table_id_t, std::vector<common::column_id_t>>
            tableIDToScanColumnIds,
        std::unique_ptr<PhysicalOperator> prevOperator, uint32_t id,
        const std::string& paramsString)
        : ScanTable{PhysicalOperatorType::SCAN_MULTI_NODE_TABLES, inVectorPos,
              std::move(outVectorsPos), std::move(prevOperator), id, paramsString},
          tables{std::move(tables)}, tableIDToScanColumnIds{std::move(tableIDToScanColumnIds)} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanMultiNodeTables>(inVectorPos, outVectorsPos, tables,
            tableIDToScanColumnIds, children[0]->clone(), id, paramsString);
    }

private:
    std::unordered_map<common::table_id_t, storage::NodeTable*> tables;
    std::unordered_map<common::table_id_t, std::vector<common::column_id_t>> tableIDToScanColumnIds;
};

} // namespace processor
} // namespace kuzu
