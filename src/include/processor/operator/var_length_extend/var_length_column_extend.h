#pragma once

#include "processor/operator/physical_operator.h"
#include "processor/operator/var_length_extend/var_length_adj_list_extend.h"
#include "processor/result/result_set.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"

namespace kuzu {
namespace processor {

struct ColumnExtendDFSLevelInfo : DFSLevelInfo {
    ColumnExtendDFSLevelInfo(uint8_t level, ExecutionContext& context)
        : DFSLevelInfo(level, context), hasBeenExtended{false} {}

    void reset();

    bool hasBeenExtended;
};

class VarLengthColumnExtend : public VarLengthExtend {
public:
    VarLengthColumnExtend(const DataPos& boundNodeDataPos, const DataPos& nbrNodeDataPos,
        storage::BaseColumnOrList* storage, uint8_t lowerBound, uint8_t upperBound,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : VarLengthExtend(PhysicalOperatorType::VAR_LENGTH_COLUMN_EXTEND, boundNodeDataPos,
              nbrNodeDataPos, storage, lowerBound, upperBound, std::move(child), id, paramsString) {
    }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<VarLengthColumnExtend>(boundNodeDataPos, nbrNodeDataPos, storage,
            lowerBound, upperBound, children[0]->clone(), id, paramsString);
    }

private:
    // This function resets the dfsLevelInfo at level and adds the dfsLevelInfo to the
    // dfsStack if the parent has adjacent nodes. The function returns true if the
    // parent has adjacent nodes, otherwise returns false.
    bool addDFSLevelToStackIfParentExtends(common::ValueVector* parentValueVector, uint8_t level);
};

} // namespace processor
} // namespace kuzu
