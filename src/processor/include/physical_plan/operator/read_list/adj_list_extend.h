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

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjListExtend>(
            inDataPos, outDataPos, (AdjLists*)lists, prevOperator->clone(), context, id);
    }
};

} // namespace processor
} // namespace graphflow
