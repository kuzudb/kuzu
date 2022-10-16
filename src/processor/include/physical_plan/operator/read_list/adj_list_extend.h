#pragma once

#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ReadList {

public:
    AdjListExtend(const DataPos& inDataPos, const DataPos& outDataPos, AdjLists* adjLists,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : ReadList{inDataPos, outDataPos, adjLists, move(child), id, paramsString} {}

    PhysicalOperatorType getOperatorType() override { return LIST_EXTEND; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjListExtend>(inDataPos, outDataPos,
            (AdjLists*)listsWithAdjAndPropertyListsUpdateStore, children[0]->clone(), id,
            paramsString);
    }
};

} // namespace processor
} // namespace graphflow
