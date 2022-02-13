#pragma once

#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/scan_column/scan_column.h"
#include "src/storage/include/data_structure/column.h"

namespace graphflow {
namespace processor {

class AdjColumnExtend : public ScanSingleColumn, public FilteringOperator {

public:
    AdjColumnExtend(const DataPos& inputNodeIDVectorPos, const DataPos& outputNodeIDVectorPos,
        Column* nodeIDColumn, unique_ptr<PhysicalOperator> child, ExecutionContext& context,
        uint32_t id)
        : ScanSingleColumn{inputNodeIDVectorPos, outputNodeIDVectorPos, move(child), context, id},
          FilteringOperator(), nodeIDColumn{nodeIDColumn} {}

    PhysicalOperatorType getOperatorType() override { return COLUMN_EXTEND; }

    shared_ptr<ResultSet> initResultSet() override;

    void reInitToRerunSubPlan() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjColumnExtend>(
            inputNodeIDVectorPos, outputVectorPos, nodeIDColumn, children[0]->clone(), context, id);
    }

private:
    Column* nodeIDColumn;
};

} // namespace processor
} // namespace graphflow
