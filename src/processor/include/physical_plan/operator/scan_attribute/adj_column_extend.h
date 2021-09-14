#pragma once

#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_column.h"

namespace graphflow {
namespace processor {

class AdjColumnExtend : public ScanColumn, public FilteringOperator {

public:
    AdjColumnExtend(const DataPos& inDataPos, const DataPos& outDataPos, Column* column,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : ScanColumn{inDataPos, outDataPos, column, move(prevOperator), context, id},
          FilteringOperator() {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    void reInitialize() override;

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjColumnExtend>(
            inDataPos, outDataPos, column, prevOperator->clone(), context, id);
    }
};

} // namespace processor
} // namespace graphflow
