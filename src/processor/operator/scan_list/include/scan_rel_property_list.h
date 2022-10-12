#pragma once

#include "src/processor/operator/scan_list/include/scan_list.h"

namespace graphflow {
namespace processor {

class ScanRelPropertyList : public ScanList {

public:
    ScanRelPropertyList(const DataPos& inDataPos, const DataPos& outDataPos,
        ListsWithAdjAndPropertyListsUpdateStore* listsWithAdjAndPropertyListsUpdateStore,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : ScanList{inDataPos, outDataPos, listsWithAdjAndPropertyListsUpdateStore, move(child), id,
              paramsString} {}

    inline PhysicalOperatorType getOperatorType() override { return SCAN_REL_PROPERTY; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanRelPropertyList>(inDataPos, outDataPos,
            listsWithAdjAndPropertyListsUpdateStore, children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace graphflow
