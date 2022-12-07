#pragma once

#include <utility>

#include "binder/expression/expression.h"
#include "common/vector/value_vector_utils.h"
#include "expression_evaluator/base_evaluator.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/source_operator.h"
#include "storage/storage_structure/column.h"
#include "storage/storage_structure/lists/lists.h"

using namespace kuzu::evaluator;

namespace kuzu {
namespace processor {

class ShortestPath : public PhysicalOperator {
public:
    ShortestPath(const DataPos& srcDataPos, const DataPos& destDataPos, vector<Column*> columns,
        uint64_t lowerBound, uint64_t upperBound, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, srcDataPos{srcDataPos},
          destDataPos{destDataPos}, columns{move(columns)}, lowerBound{lowerBound},
          upperBound{upperBound} {}

    ShortestPath(const DataPos& srcDataPos, const DataPos& destDataPos, vector<AdjLists*> lists,
        uint64_t lowerBound, uint64_t upperBound, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, srcDataPos{srcDataPos},
          destDataPos{destDataPos}, lists{move(lists)}, lowerBound{lowerBound}, upperBound{
                                                                                    upperBound} {}

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::SHORTEST_PATH;
    }

    bool getNextTuplesInternal() override;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    inline unique_ptr<PhysicalOperator> clone() override {
        if (!columns.empty()) {
            return make_unique<ShortestPath>(srcDataPos, destDataPos, columns, lowerBound,
                upperBound, children[0]->clone(), id, paramsString);
        } else {
            return make_unique<ShortestPath>(srcDataPos, destDataPos, lists, lowerBound, upperBound,
                children[0]->clone(), id, paramsString);
        }
    }

private:
    DataPos srcDataPos;
    DataPos destDataPos;
    vector<Column*> columns;
    vector<AdjLists*> lists;
    uint64_t lowerBound;
    uint64_t upperBound;
    shared_ptr<ValueVector> srcValueVector;
    shared_ptr<ValueVector> destValueVector;
};

} // namespace processor
} // namespace kuzu