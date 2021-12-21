#pragma once

#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

class ReadRelPropertyList : public ReadList {

public:
    ReadRelPropertyList(const DataPos& inDataPos, const DataPos& outDataPos, Lists* lists,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : ReadList{inDataPos, outDataPos, lists, move(prevOperator), context, id,
              false /* is not adj list */} {}

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ReadRelPropertyList>(
            inDataPos, outDataPos, lists, prevOperator->clone(), context, id);
    }
};

} // namespace processor
} // namespace graphflow
