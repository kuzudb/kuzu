#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/result/result_set.h"
#include "src/storage/include/storage_structure/lists/lists.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct DFSLevelInfo {
    DFSLevelInfo(uint8_t level, ExecutionContext& context);

    void reset(uint64_t parent);

    const uint8_t level;
    uint64_t parent;
    uint64_t childrenIdx;
    bool hasBeenOutput;
    shared_ptr<ValueVector> children;
    unique_ptr<LargeListHandle> largeListHandle;
};

class VarLengthAdjListExtend : public PhysicalOperator {

public:
    VarLengthAdjListExtend(const DataPos& boundNodeDataPos, const DataPos& nbrNodeDataPos,
        AdjLists* adjLists, uint8_t lowerBound, uint8_t upperBound,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id);

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    void reInitToRerunSubPlan() {
        while (!dfsStack.empty()) {
            dfsStack.pop();
        }
        for (auto& dfsLevelInfo : dfsLevelInfos) {
            dfsLevelInfo->largeListHandle->reset();
        }
        children[0]->reInitToRerunSubPlan();
    }

    PhysicalOperatorType getOperatorType() override { return VAR_LENGTH_EXTEND; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<VarLengthAdjListExtend>(boundNodeDataPos, nbrNodeDataPos, adjLists,
            lowerBound, upperBound, children[0]->clone(), context, id);
    }

private:
    // This function resets the dfsLevelInfo at level and adds the dfsLevelInfo to the
    // dfsStack if the parent has adjacent nodes. The function returns true if the
    // parent has adjacent nodes, otherwise returns false.
    bool addDFSLevelToStackIfParentExtends(uint64_t parent, uint8_t level);

    bool getNextBatchOfNbrNodes(shared_ptr<DFSLevelInfo>& dfsLevel) const;

private:
    DataPos boundNodeDataPos;
    DataPos nbrNodeDataPos;
    AdjLists* adjLists;
    uint8_t lowerBound;
    uint8_t upperBound;
    shared_ptr<ValueVector> boundNodeValueVector;
    shared_ptr<ValueVector> nbrNodeValueVector;
    stack<shared_ptr<DFSLevelInfo>> dfsStack;
    // The VarLenExtend has the invariant that at any point in time, there will be one DSFLevelInfo
    // in the dfsStack for each level. dfsLevelInfos is a pool of DFSLevelInfos that holds
    // upperBound many of them (which is the maximum we need at any point in time). This allows us
    // to avoid creating many DFSLevelInfos as we perform the dfs.
    vector<shared_ptr<DFSLevelInfo>> dfsLevelInfos;
};

} // namespace processor
} // namespace graphflow
