#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Limit : public PhysicalOperator {

public:
    Limit(uint64_t limitNumber, shared_ptr<atomic_uint64_t> counter, uint32_t dataChunkToSelectPos,
        unordered_set<uint32_t> dataChunksPosInScope, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, limitNumber{limitNumber},
          counter{move(counter)}, dataChunkToSelectPos{dataChunkToSelectPos},
          dataChunksPosInScope(move(dataChunksPosInScope)) {}

    PhysicalOperatorType getOperatorType() override { return LIMIT; }

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Limit>(limitNumber, counter, dataChunkToSelectPos, dataChunksPosInScope,
            children[0]->clone(), id, paramsString);
    }

private:
    uint64_t limitNumber;
    shared_ptr<atomic_uint64_t> counter;
    uint32_t dataChunkToSelectPos;
    unordered_set<uint32_t> dataChunksPosInScope;
};

} // namespace processor
} // namespace kuzu
