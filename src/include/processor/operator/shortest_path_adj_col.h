#pragma once

#include "binder/expression/expression.h"
#include "common/vector/value_vector_utils.h"
#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/base_shortest_path.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/source_operator.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class ShortestPathAdjCol : public BaseShortestPath {
public:
    ShortestPathAdjCol(const DataPos& srcDataPos, const DataPos& destDataPos,
        BaseColumnOrList* columns, uint64_t lowerBound, uint64_t upperBound,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseShortestPath{srcDataPos, destDataPos, columns, lowerBound, upperBound, move(child),
              id, paramsString} {}

    bool getNextTuplesInternal() override;

    PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::SHORTEST_PATH_ADJ_COL;
    }

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ShortestPathAdjCol>(srcDataPos, destDataPos, storage, lowerBound,
            upperBound, children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
