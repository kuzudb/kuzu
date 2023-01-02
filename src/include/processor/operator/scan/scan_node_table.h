#pragma once

#include "processor/operator/scan/scan_columns.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class ScanSingleNodeTable : public ScanColumns {
public:
    ScanSingleNodeTable(const DataPos& inVectorPos, vector<DataPos> outVectorsPos, NodeTable* table,
        vector<uint32_t> propertyColumnIds, unique_ptr<PhysicalOperator> prevOperator, uint32_t id,
        const string& paramsString)
        : ScanColumns{inVectorPos, std::move(outVectorsPos), std::move(prevOperator), id,
              paramsString},
          table{table}, propertyColumnIds{std::move(propertyColumnIds)} {}

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanSingleNodeTable>(inputNodeIDVectorPos, outPropertyVectorsPos, table,
            propertyColumnIds, children[0]->clone(), id, paramsString);
    }

private:
    NodeTable* table;
    vector<uint32_t> propertyColumnIds;
};

class ScanMultiNodeTables : public ScanColumns {
public:
    ScanMultiNodeTables(const DataPos& inVectorPos, vector<DataPos> outVectorsPos,
        unordered_map<table_id_t, NodeTable*> tables,
        unordered_map<table_id_t, vector<uint32_t>> tableIDToScanColumnIds,
        unique_ptr<PhysicalOperator> prevOperator, uint32_t id, const string& paramsString)
        : ScanColumns{inVectorPos, std::move(outVectorsPos), std::move(prevOperator), id,
              paramsString},
          tables{std::move(tables)}, tableIDToScanColumnIds{std::move(tableIDToScanColumnIds)} {}

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanMultiNodeTables>(inputNodeIDVectorPos, outPropertyVectorsPos, tables,
            tableIDToScanColumnIds, children[0]->clone(), id, paramsString);
    }

private:
    unordered_map<table_id_t, NodeTable*> tables;
    unordered_map<table_id_t, vector<uint32_t>> tableIDToScanColumnIds;
};

} // namespace processor
} // namespace kuzu
