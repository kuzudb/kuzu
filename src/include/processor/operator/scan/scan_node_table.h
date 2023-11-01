#pragma once

#include "processor/operator/scan/scan_columns.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

struct ScanNodeTableInfo {
    storage::NodeTable* table;
    std::vector<common::column_id_t> columnIDs;

    ScanNodeTableInfo(storage::NodeTable* table, std::vector<common::column_id_t> columnIDs)
        : table{table}, columnIDs{std::move(columnIDs)} {}
    ScanNodeTableInfo(const ScanNodeTableInfo& other)
        : table{other.table}, columnIDs{other.columnIDs} {}

    inline std::unique_ptr<ScanNodeTableInfo> copy() const {
        return std::make_unique<ScanNodeTableInfo>(*this);
    }
};

class ScanSingleNodeTable : public ScanColumns {
public:
    ScanSingleNodeTable(std::unique_ptr<ScanNodeTableInfo> info, const DataPos& inVectorPos,
        std::vector<DataPos> outVectorsPos, std::unique_ptr<PhysicalOperator> prevOperator,
        uint32_t id, const std::string& paramsString)
        : ScanColumns{inVectorPos, std::move(outVectorsPos), std::move(prevOperator), id,
              paramsString},
          info{std::move(info)} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanSingleNodeTable>(info->copy(), inputNodeIDVectorPos,
            outPropertyVectorsPos, children[0]->clone(), id, paramsString);
    }

private:
    std::unique_ptr<ScanNodeTableInfo> info;
};

class ScanMultiNodeTables : public ScanColumns {
public:
    ScanMultiNodeTables(const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
        std::unordered_map<common::table_id_t, std::unique_ptr<ScanNodeTableInfo>> tables,
        std::unique_ptr<PhysicalOperator> prevOperator, uint32_t id,
        const std::string& paramsString)
        : ScanColumns{inVectorPos, std::move(outVectorsPos), std::move(prevOperator), id,
              paramsString},
          tables{std::move(tables)} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        auto clonedTables =
            std::unordered_map<common::table_id_t, std::unique_ptr<ScanNodeTableInfo>>{};
        for (const auto& table : tables) {
            clonedTables.emplace(table.first, table.second->copy());
        }
        return make_unique<ScanMultiNodeTables>(inputNodeIDVectorPos, outPropertyVectorsPos,
            std::move(clonedTables), children[0]->clone(), id, paramsString);
    }

private:
    std::unordered_map<common::table_id_t, std::unique_ptr<ScanNodeTableInfo>> tables;
};

} // namespace processor
} // namespace kuzu
