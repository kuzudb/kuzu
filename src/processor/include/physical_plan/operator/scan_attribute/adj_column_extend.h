#pragma once

#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"
#include "src/storage/include/data_structure/column.h"

namespace graphflow {
namespace processor {

class AdjColumnExtend : public ScanAttribute, public FilteringOperator {

public:
    AdjColumnExtend(const DataPos& inDataPos, const DataPos& outDataPos, Column* column,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : ScanAttribute{inDataPos, outDataPos, move(child), context, id},
          FilteringOperator(), column{column} {}

    PhysicalOperatorType getOperatorType() override { return COLUMN_EXTEND; }

    shared_ptr<ResultSet> initResultSet() override;

    void reInitToRerunSubPlan() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjColumnExtend>(
            inDataPos, outDataPos, column, children[0]->clone(), context, id);
    }

private:
    Column* column;
};

} // namespace processor
} // namespace graphflow
