#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/result/result_set.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

struct DFSLevelInfo {
    DFSLevelInfo(uint8_t level, ExecutionContext& context)
        : level{level}, hasBeenOutput{false}, children{std::make_unique<common::ValueVector>(
                                                  common::INTERNAL_ID, context.memoryManager)} {};
    const uint8_t level;
    bool hasBeenOutput;
    std::unique_ptr<common::ValueVector> children;
};

class VarLengthExtend : public PhysicalOperator {
public:
    VarLengthExtend(PhysicalOperatorType operatorType, const DataPos& boundNodeDataPos,
        const DataPos& nbrNodeDataPos, storage::BaseColumnOrList* storage, uint8_t lowerBound,
        uint8_t upperBound, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{operatorType, std::move(child), id, paramsString},
          boundNodeDataPos{boundNodeDataPos}, nbrNodeDataPos{nbrNodeDataPos}, storage{storage},
          lowerBound{lowerBound}, upperBound{upperBound} {
        dfsLevelInfos.resize(upperBound);
    }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    DataPos boundNodeDataPos;
    DataPos nbrNodeDataPos;
    storage::BaseColumnOrList* storage;
    uint8_t lowerBound;
    uint8_t upperBound;
    common::ValueVector* boundNodeValueVector;
    common::ValueVector* nbrNodeValueVector;
    std::stack<std::shared_ptr<DFSLevelInfo>> dfsStack;
    // The VarLenExtend has the invariant that at any point in time, there will be one DSFLevelInfo
    // in the dfsStack for each level. dfsLevelInfos is a pool of DFSLevelInfos that holds
    // upperBound many of them (which is the maximum we need at any point in time). This allows us
    // to avoid creating many DFSLevelInfos as we perform the dfs.
    std::vector<std::shared_ptr<DFSLevelInfo>> dfsLevelInfos;
};

} // namespace processor
} // namespace kuzu
