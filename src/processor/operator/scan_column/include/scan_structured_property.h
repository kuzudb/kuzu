#pragma once

#include "src/processor/operator/scan_column/include/scan_column.h"
#include "src/storage/storage_structure/include/column.h"

namespace kuzu {
namespace processor {

class ScanSingleTablePropertyColumn : public ScanMultipleColumns {
public:
    ScanSingleTablePropertyColumn(const DataPos& inVectorPos, vector<DataPos> outVectorsPos,
        vector<DataType> outDataTypes, vector<Column*> columns,
        unique_ptr<PhysicalOperator> prevOperator, uint32_t id, const string& paramsString)
        : ScanMultipleColumns{inVectorPos, std::move(outVectorsPos), std::move(outDataTypes),
              std::move(prevOperator), id, paramsString},
          columns{std::move(columns)} {}

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanSingleTablePropertyColumn>(inputNodeIDVectorPos, outVectorsPos,
            outDataTypes, columns, children[0]->clone(), id, paramsString);
    }

private:
    vector<Column*> columns;
};

class ScanMultiTablePropertyColumn : public ScanMultipleColumns {
public:
    ScanMultiTablePropertyColumn(const DataPos& inVectorPos, vector<DataPos> outVectorsPos,
        vector<DataType> outDataTypes, vector<unordered_map<table_id_t, Column*>> tableIDToColumns,
        unique_ptr<PhysicalOperator> prevOperator, uint32_t id, const string& paramsString)
        : ScanMultipleColumns{inVectorPos, std::move(outVectorsPos), std::move(outDataTypes),
              std::move(prevOperator), id, paramsString},
          tableIDToColumns{std::move(tableIDToColumns)} {}

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanMultiTablePropertyColumn>(inputNodeIDVectorPos, outVectorsPos,
            outDataTypes, tableIDToColumns, children[0]->clone(), id, paramsString);
    }

private:
    vector<unordered_map<table_id_t, Column*>> tableIDToColumns;
};

} // namespace processor
} // namespace kuzu
