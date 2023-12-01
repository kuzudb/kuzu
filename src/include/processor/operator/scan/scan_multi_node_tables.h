#pragma once

#include "processor/operator/scan/scan_node_table.h"

namespace kuzu {
namespace processor {

class ScanMultiNodeTables : public ScanTable {
public:
    ScanMultiNodeTables(const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
        std::unordered_map<common::table_id_t, std::unique_ptr<ScanNodeTableInfo>> tables,
        std::unique_ptr<PhysicalOperator> prevOperator, uint32_t id,
        const std::string& paramsString)
        : ScanTable{PhysicalOperatorType::SCAN_MULTI_NODE_TABLES, inVectorPos,
              std::move(outVectorsPos), std::move(prevOperator), id, paramsString},
          tables{std::move(tables)} {}

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        ScanTable::initLocalStateInternal(resultSet, context);
        for (auto& [tableID, _] : tables) {
            readStates[tableID] = std::make_unique<storage::TableReadState>();
        }
    }
    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        std::unordered_map<common::table_id_t, std::unique_ptr<ScanNodeTableInfo>> clonedTables;
        for (const auto& table : tables) {
            clonedTables[table.first] = table.second->copy();
        }
        return make_unique<ScanMultiNodeTables>(inVectorPos, outVectorsPos, std::move(clonedTables),
            children[0]->clone(), id, paramsString);
    }

private:
    std::unordered_map<common::table_id_t, std::unique_ptr<ScanNodeTableInfo>> tables;
    std::unordered_map<common::table_id_t, std::unique_ptr<storage::TableReadState>> readStates;
};

} // namespace processor
} // namespace kuzu
