#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Flatten : public PhysicalOperator {
public:
    Flatten(uint32_t dataChunkToFlattenPos, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::FLATTEN, std::move(child), id, paramsString},
          dataChunkToFlattenPos{dataChunkToFlattenPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Flatten>(dataChunkToFlattenPos, children[0]->clone(), id, paramsString);
    }

private:
    bool isCurrIdxInitialOrLast() {
        return dataChunkToFlatten->state->currIdx == -1 ||
               dataChunkToFlatten->state->currIdx == (unFlattenedSelVector->selectedSize - 1);
    }

private:
    uint32_t dataChunkToFlattenPos;
    std::shared_ptr<DataChunk> dataChunkToFlatten;
    std::shared_ptr<SelectionVector> unFlattenedSelVector;
    std::shared_ptr<SelectionVector> flattenedSelVector;
};

} // namespace processor
} // namespace kuzu
