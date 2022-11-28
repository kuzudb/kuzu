#pragma once

#include "processor/operator/var_length_extend/var_length_extend.h"
#include "storage/storage_structure/lists/lists.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

struct AdjListExtendDFSLevelInfo : DFSLevelInfo {
    AdjListExtendDFSLevelInfo(uint8_t level, ExecutionContext& context);

    void reset(uint64_t parent);

    uint64_t parent;
    uint64_t childrenIdx;
    shared_ptr<ListSyncState> listSyncState;
    shared_ptr<ListHandle> listHandle;
};

class VarLengthAdjListExtend : public VarLengthExtend {

public:
    VarLengthAdjListExtend(const DataPos& boundNodeDataPos, const DataPos& nbrNodeDataPos,
        BaseColumnOrList* adjLists, uint8_t lowerBound, uint8_t upperBound,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : VarLengthExtend(boundNodeDataPos, nbrNodeDataPos, adjLists, lowerBound, upperBound,
              move(child), id, paramsString) {}

    PhysicalOperatorType getOperatorType() override { return VAR_LENGTH_ADJ_LIST_EXTEND; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<VarLengthAdjListExtend>(boundNodeDataPos, nbrNodeDataPos, storage,
            lowerBound, upperBound, children[0]->clone(), id, paramsString);
    }

private:
    // This function resets the dfsLevelInfo at level and adds the dfsLevelInfo to the
    // dfsStack if the parent has adjacent nodes. The function returns true if the
    // parent has adjacent nodes, otherwise returns false.
    bool addDFSLevelToStackIfParentExtends(uint64_t parent, uint8_t level);

    bool getNextBatchOfNbrNodes(shared_ptr<AdjListExtendDFSLevelInfo>& dfsLevel) const;
};

} // namespace processor
} // namespace kuzu
