#pragma once

#include "processor/operator/filtering_operator.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Skip : public PhysicalOperator, public FilteringOperator {

public:
    Skip(uint64_t skipNumber, shared_ptr<atomic_uint64_t> counter, uint32_t dataChunkToSelectPos,
        unordered_set<uint32_t> dataChunksPosInScope, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString},
          FilteringOperator{1 /* numStatesToSave */}, skipNumber{skipNumber}, counter{move(
                                                                                  counter)},
          dataChunkToSelectPos{dataChunkToSelectPos}, dataChunksPosInScope{
                                                          move(dataChunksPosInScope)} {}

    PhysicalOperatorType getOperatorType() override { return SKIP; }

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Skip>(skipNumber, counter, dataChunkToSelectPos, dataChunksPosInScope,
            children[0]->clone(), id, paramsString);
    }

private:
    uint64_t skipNumber;
    shared_ptr<atomic_uint64_t> counter;
    uint32_t dataChunkToSelectPos;
    unordered_set<uint32_t> dataChunksPosInScope;
};

} // namespace processor
} // namespace kuzu
