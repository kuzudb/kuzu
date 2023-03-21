#pragma once

#include "processor/operator/filtering_operator.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Flatten : public PhysicalOperator, SelVectorOverWriter {
public:
    Flatten(uint32_t dataChunkToFlattenPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::FLATTEN, std::move(child), id, paramsString},
          dataChunkToFlattenPos{dataChunkToFlattenPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Flatten>(dataChunkToFlattenPos, children[0]->clone(), id, paramsString);
    }

private:
    inline bool isCurrIdxInitialOrLast() {
        return dataChunkToFlatten->state->currIdx == -1 ||
               dataChunkToFlatten->state->currIdx == (prevSelVector->selectedSize - 1);
    }
    void resetToCurrentSelVector(std::shared_ptr<common::SelectionVector>& selVector) override;

private:
    uint32_t dataChunkToFlattenPos;
    std::shared_ptr<common::DataChunk> dataChunkToFlatten;
};

} // namespace processor
} // namespace kuzu
