#pragma once

#include "src/processor/include/physical_plan/operator/var_length_extend/var_length_extend.h"
#include "src/storage/include/storage_structure/lists/lists.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct AdjListExtendDFSLevelInfo : DFSLevelInfo {
    AdjListExtendDFSLevelInfo(uint8_t level, ExecutionContext& context);

    void reset(uint64_t parent);

    uint64_t parent;
    uint64_t childrenIdx;
    unique_ptr<LargeListHandle> largeListHandle;
};

class VarLengthAdjListExtend : public VarLengthExtend {

public:
    VarLengthAdjListExtend(const DataPos& boundNodeDataPos, const DataPos& nbrNodeDataPos,
        StorageStructure* adjLists, uint8_t lowerBound, uint8_t upperBound,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id);

    bool getNextTuples() override;

    void reInitToRerunSubPlan() override;

    PhysicalOperatorType getOperatorType() override { return VAR_LENGTH_ADJ_LIST_EXTEND; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<VarLengthAdjListExtend>(boundNodeDataPos, nbrNodeDataPos, storage,
            lowerBound, upperBound, children[0]->clone(), context, id);
    }

private:
    // This function resets the dfsLevelInfo at level and adds the dfsLevelInfo to the
    // dfsStack if the parent has adjacent nodes. The function returns true if the
    // parent has adjacent nodes, otherwise returns false.
    bool addDFSLevelToStackIfParentExtends(uint64_t parent, uint8_t level);

    bool getNextBatchOfNbrNodes(shared_ptr<AdjListExtendDFSLevelInfo>& dfsLevel) const;
};

} // namespace processor
} // namespace graphflow
