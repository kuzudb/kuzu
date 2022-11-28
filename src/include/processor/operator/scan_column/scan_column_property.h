#pragma once

#include "processor/operator/scan_column/scan_column.h"
#include "storage/storage_structure/column.h"

namespace kuzu {
namespace processor {

class ScanSingleTableProperties : public ScanMultipleColumns {
public:
    ScanSingleTableProperties(const DataPos& inVectorPos, vector<DataPos> outVectorsPos,
        vector<DataType> outDataTypes, vector<Column*> columns,
        unique_ptr<PhysicalOperator> prevOperator, uint32_t id, const string& paramsString)
        : ScanMultipleColumns{inVectorPos, std::move(outVectorsPos), std::move(outDataTypes),
              std::move(prevOperator), id, paramsString},
          columns{std::move(columns)} {}

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanSingleTableProperties>(inputNodeIDVectorPos, outVectorsPos,
            outDataTypes, columns, children[0]->clone(), id, paramsString);
    }

private:
    vector<Column*> columns;
};

class ScanMultiTableProperties : public ScanMultipleColumns {
public:
    ScanMultiTableProperties(const DataPos& inVectorPos, vector<DataPos> outVectorsPos,
        vector<DataType> outDataTypes, vector<unordered_map<table_id_t, Column*>> tableIDToColumns,
        unique_ptr<PhysicalOperator> prevOperator, uint32_t id, const string& paramsString)
        : ScanMultipleColumns{inVectorPos, std::move(outVectorsPos), std::move(outDataTypes),
              std::move(prevOperator), id, paramsString},
          tableIDToColumns{std::move(tableIDToColumns)} {}

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanMultiTableProperties>(inputNodeIDVectorPos, outVectorsPos,
            outDataTypes, tableIDToColumns, children[0]->clone(), id, paramsString);
    }

private:
    vector<unordered_map<table_id_t, Column*>> tableIDToColumns;
};

} // namespace processor
} // namespace kuzu
