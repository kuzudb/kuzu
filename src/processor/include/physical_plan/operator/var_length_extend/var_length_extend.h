#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/result/result_set.h"
#include "src/storage/storage_structure/include/lists/lists.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct DFSLevelInfo {
    DFSLevelInfo(uint8_t level, ExecutionContext& context)
        : level{level}, hasBeenOutput{false}, children{make_shared<ValueVector>(
                                                  NODE_ID, context.memoryManager)} {};
    const uint8_t level;
    bool hasBeenOutput;
    shared_ptr<ValueVector> children;
};

class VarLengthExtend : public PhysicalOperator {

public:
    VarLengthExtend(const DataPos& boundNodeDataPos, const DataPos& nbrNodeDataPos,
        BaseColumnOrList* storage, uint8_t lowerBound, uint8_t upperBound,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString);

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void reInitToRerunSubPlan() override;

protected:
    DataPos boundNodeDataPos;
    DataPos nbrNodeDataPos;
    BaseColumnOrList* storage;
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
