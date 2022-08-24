#pragma once

#include "src/processor/include/physical_plan/operator/filtering_operator.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Sorter : public PhysicalOperator, FilteringOperator {

public:
    Sorter(DataPos vectorToSortPos, unique_ptr<PhysicalOperator> child, uint32_t id,
           const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, FilteringOperator{1},
          vectorToSortPos{move(vectorToSortPos)} {}

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    inline PhysicalOperatorType getOperatorType() override { return SORTER; }
    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Sorter>(vectorToSortPos, children[0]->clone(), id, paramsString);
    }

private:
    DataPos vectorToSortPos;
    shared_ptr<ValueVector> vectorToSort;
};

} // namespace processor
} // namespace graphflow
