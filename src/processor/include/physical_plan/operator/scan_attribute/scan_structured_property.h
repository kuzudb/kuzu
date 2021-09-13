#pragma once

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_column.h"

namespace graphflow {
namespace processor {

class ScanStructuredProperty : public ScanColumn {

public:
    ScanStructuredProperty(const DataPos& inDataPos, const DataPos& outDataPos, Column* column,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : ScanColumn{inDataPos, outDataPos, column, move(prevOperator), context, id} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanStructuredProperty>(
            inDataPos, outDataPos, column, prevOperator->clone(), context, id);
    }
};

} // namespace processor
} // namespace graphflow
