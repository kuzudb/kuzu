#pragma once

#include "processor/operator/scan_list/scan_list.h"

namespace kuzu {
namespace processor {

class ScanRelPropertyList : public ScanList {

public:
    ScanRelPropertyList(const DataPos& inDataPos, const DataPos& outDataPos, Lists* lists,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : ScanList{inDataPos, outDataPos, lists, move(child), id, paramsString} {}

    inline PhysicalOperatorType getOperatorType() override { return SCAN_REL_PROPERTY; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanRelPropertyList>(
            inDataPos, outDataPos, lists, children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
