#pragma once

#include "processor/operator/scan_list/scan_list.h"

namespace kuzu {
namespace processor {

class AdjListExtend : public ScanList {

public:
    AdjListExtend(const DataPos& inDataPos, const DataPos& outDataPos, AdjLists* adjLists,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : ScanList{inDataPos, outDataPos, adjLists, move(child), id, paramsString} {}

    inline PhysicalOperatorType getOperatorType() override { return LIST_EXTEND; }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<AdjListExtend>(
            inDataPos, outDataPos, (AdjLists*)lists, children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
