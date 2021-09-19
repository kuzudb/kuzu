#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Limit : public PhysicalOperator {

public:
    Limit(uint64_t limitNumber, shared_ptr<atomic_uint64_t> counter, uint32_t dataChunkToSelectPos,
        vector<uint32_t> dataChunksToLimitPos, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), LIMIT, context, id}, limitNumber{limitNumber},
          counter{move(counter)}, dataChunkToSelectPos{dataChunkToSelectPos},
          dataChunksToLimitPos(move(dataChunksToLimitPos)) {}

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Limit>(limitNumber, counter, dataChunkToSelectPos, dataChunksToLimitPos,
            prevOperator->clone(), context, id);
    }

private:
    uint64_t limitNumber;
    shared_ptr<atomic_uint64_t> counter;
    uint32_t dataChunkToSelectPos;
    vector<uint32_t> dataChunksToLimitPos;
};

} // namespace processor
} // namespace graphflow
