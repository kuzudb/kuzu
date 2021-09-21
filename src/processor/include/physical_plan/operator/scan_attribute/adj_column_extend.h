#pragma once

#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"
#include "src/storage/include/data_structure/column.h"

namespace graphflow {
namespace processor {

class AdjColumnExtend : public ScanAttribute, public FilteringOperator {

public:
    AdjColumnExtend(const DataPos& inDataPos, const DataPos& outDataPos, Column* column,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : ScanAttribute{inDataPos, outDataPos, move(prevOperator), context, id},
          FilteringOperator(), column{column} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    void reInitialize() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjColumnExtend>(
            inDataPos, outDataPos, column, prevOperator->clone(), context, id);
    }

private:
    Column* column;
};

} // namespace processor
} // namespace graphflow
