#pragma once

#include "processor/operator/scan_column/scan_column.h"
#include "storage/storage_structure/column.h"

namespace kuzu {
namespace processor {

class ScanSingleNodeTableProperties : public ScanMultipleColumns {
public:
    ScanSingleNodeTableProperties(const DataPos& inVectorPos, vector<DataPos> outVectorsPos,
        vector<Column*> propertyColumns, unique_ptr<PhysicalOperator> prevOperator, uint32_t id,
        const string& paramsString)
        : ScanMultipleColumns{inVectorPos, std::move(outVectorsPos), std::move(prevOperator), id,
              paramsString},
          propertyColumns{std::move(propertyColumns)} {}

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanSingleNodeTableProperties>(inputNodeIDVectorPos,
            outPropertyVectorsPos, propertyColumns, children[0]->clone(), id, paramsString);
    }

private:
    vector<Column*> propertyColumns;
};

class ScanMultiNodeTableProperties : public ScanMultipleColumns {
public:
    ScanMultiNodeTableProperties(const DataPos& inVectorPos, vector<DataPos> outVectorsPos,
        unordered_map<table_id_t, vector<Column*>> tableIDToPropertyColumns,
        unique_ptr<PhysicalOperator> prevOperator, uint32_t id, const string& paramsString)
        : ScanMultipleColumns{inVectorPos, std::move(outVectorsPos), std::move(prevOperator), id,
              paramsString},
          tableIDToPropertyColumns{std::move(tableIDToPropertyColumns)} {}

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanMultiNodeTableProperties>(inputNodeIDVectorPos,
            outPropertyVectorsPos, tableIDToPropertyColumns, children[0]->clone(), id,
            paramsString);
    }

private:
    unordered_map<table_id_t, vector<Column*>> tableIDToPropertyColumns;
};

} // namespace processor
} // namespace kuzu
