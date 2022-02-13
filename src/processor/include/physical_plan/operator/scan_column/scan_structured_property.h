#pragma once

#include "src/processor/include/physical_plan/operator/scan_column/scan_column.h"
#include "src/storage/include/data_structure/column.h"

namespace graphflow {
namespace processor {

class ScanStructuredProperty : public ScanMultipleColumns {

public:
    ScanStructuredProperty(const DataPos& inputNodeIDVectorPos,
        vector<DataPos> outputPropertyVectorsPos, vector<Column*> propertyColumns,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : ScanMultipleColumns{inputNodeIDVectorPos, move(outputPropertyVectorsPos),
              move(prevOperator), context, id},
          propertyColumns{move(propertyColumns)} {}

    PhysicalOperatorType getOperatorType() override { return SCAN_STRUCTURED_PROPERTY; }

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanStructuredProperty>(inputNodeIDVectorPos, outputVectorsPos,
            propertyColumns, children[0]->clone(), context, id);
    }

private:
    vector<Column*> propertyColumns;
};

} // namespace processor
} // namespace graphflow
