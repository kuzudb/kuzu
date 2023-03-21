#pragma once

#include "processor/operator/scan/scan_columns.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class ScanSingleNodeTable : public ScanColumns {
public:
    ScanSingleNodeTable(const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
        storage::NodeTable* table, std::vector<uint32_t> propertyColumnIds,
        std::unique_ptr<PhysicalOperator> prevOperator, uint32_t id,
        const std::string& paramsString)
        : ScanColumns{inVectorPos, std::move(outVectorsPos), std::move(prevOperator), id,
              paramsString},
          table{table}, propertyColumnIds{std::move(propertyColumnIds)} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanSingleNodeTable>(inputNodeIDVectorPos, outPropertyVectorsPos, table,
            propertyColumnIds, children[0]->clone(), id, paramsString);
    }

private:
    storage::NodeTable* table;
    std::vector<uint32_t> propertyColumnIds;
};

class ScanMultiNodeTables : public ScanColumns {
public:
    ScanMultiNodeTables(const DataPos& inVectorPos, std::vector<DataPos> outVectorsPos,
        std::unordered_map<common::table_id_t, storage::NodeTable*> tables,
        std::unordered_map<common::table_id_t, std::vector<uint32_t>> tableIDToScanColumnIds,
        std::unique_ptr<PhysicalOperator> prevOperator, uint32_t id,
        const std::string& paramsString)
        : ScanColumns{inVectorPos, std::move(outVectorsPos), std::move(prevOperator), id,
              paramsString},
          tables{std::move(tables)}, tableIDToScanColumnIds{std::move(tableIDToScanColumnIds)} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanMultiNodeTables>(inputNodeIDVectorPos, outPropertyVectorsPos, tables,
            tableIDToScanColumnIds, children[0]->clone(), id, paramsString);
    }

private:
    std::unordered_map<common::table_id_t, storage::NodeTable*> tables;
    std::unordered_map<common::table_id_t, std::vector<uint32_t>> tableIDToScanColumnIds;
};

} // namespace processor
} // namespace kuzu
