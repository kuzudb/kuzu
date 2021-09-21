#pragma once

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"
#include "src/storage/include/data_structure/column.h"

namespace graphflow {
namespace processor {

class ScanStructuredProperty : public ScanAttribute {

public:
    ScanStructuredProperty(const DataPos& inDataPos, const DataPos& outDataPos, Column* column,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : ScanAttribute{inDataPos, outDataPos, move(prevOperator), context, id}, column{column} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanStructuredProperty>(
            inDataPos, outDataPos, column, prevOperator->clone(), context, id);
    }

private:
    Column* column;
};

} // namespace processor
} // namespace graphflow
