#pragma once

#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ReadList {

public:
    AdjListExtend(const DataPos& inDataPos, const DataPos& outDataPos, AdjLists* lists,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : ReadList{inDataPos, outDataPos, lists, move(prevOperator), context, id,
              true /* isAdjList */} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjListExtend>(
            inDataPos, outDataPos, (AdjLists*)lists, prevOperator->clone(), context, id);
    }
};

} // namespace processor
} // namespace graphflow
