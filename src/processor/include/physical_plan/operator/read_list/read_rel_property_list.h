#pragma once

#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

class ReadRelPropertyList : public ReadList {

public:
    ReadRelPropertyList(const DataPos& inDataPos, const DataPos& outDataPos, Lists* lists,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : ReadList{inDataPos, outDataPos, lists, move(child), id, false /* is not adj list */,
              paramsString} {}

    PhysicalOperatorType getOperatorType() override { return READ_REL_PROPERTY; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ReadRelPropertyList>(
            inDataPos, outDataPos, lists, children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace graphflow
