#pragma once

#include "processor/operator/filtering_operator.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct FlattenLocalState {
    uint64_t currentIdx = 0;
    uint64_t sizeToFlatten = 0;
};

class Flatten : public PhysicalOperator, SelVectorOverWriter {
public:
    Flatten(data_chunk_pos_t dataChunkToFlattenPos, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::FLATTEN, std::move(child), id, paramsString},
          dataChunkToFlattenPos{dataChunkToFlattenPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Flatten>(dataChunkToFlattenPos, children[0]->clone(), id, paramsString);
    }

private:
    void resetToCurrentSelVector(std::shared_ptr<common::SelectionVector>& selVector) override;

private:
    data_chunk_pos_t dataChunkToFlattenPos;
    common::DataChunkState* dataChunkState;
    std::unique_ptr<FlattenLocalState> localState;
};

} // namespace processor
} // namespace kuzu
