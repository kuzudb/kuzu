#pragma once

#include "processor/operator/var_length_extend/var_length_extend.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

struct AdjListExtendDFSLevelInfo : DFSLevelInfo {
    AdjListExtendDFSLevelInfo(uint8_t level, ExecutionContext& context);

    void reset(uint64_t parent);

    uint64_t parent;
    uint64_t childrenIdx;
    std::unique_ptr<storage::ListSyncState> listSyncState;
    std::unique_ptr<storage::ListHandle> listHandle;
};

class VarLengthAdjListExtend : public VarLengthExtend {
public:
    VarLengthAdjListExtend(const DataPos& boundNodeDataPos, const DataPos& nbrNodeDataPos,
        storage::BaseColumnOrList* adjLists, uint8_t lowerBound, uint8_t upperBound,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : VarLengthExtend(PhysicalOperatorType::VAR_LENGTH_ADJ_LIST_EXTEND, boundNodeDataPos,
              nbrNodeDataPos, adjLists, lowerBound, upperBound, std::move(child), id,
              paramsString) {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<VarLengthAdjListExtend>(boundNodeDataPos, nbrNodeDataPos, storage,
            lowerBound, upperBound, children[0]->clone(), id, paramsString);
    }

private:
    // This function resets the dfsLevelInfo at level and adds the dfsLevelInfo to the
    // dfsStack if the parent has adjacent nodes. The function returns true if the
    // parent has adjacent nodes, otherwise returns false.
    bool addDFSLevelToStackIfParentExtends(uint64_t parent, uint8_t level);

    bool getNextBatchOfNbrNodes(std::shared_ptr<AdjListExtendDFSLevelInfo>& dfsLevel) const;
};

} // namespace processor
} // namespace kuzu
