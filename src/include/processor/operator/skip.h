#pragma once

#include "processor/operator/filtering_operator.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Skip : public PhysicalOperator, public SelVectorOverWriter {
public:
    Skip(uint64_t skipNumber, shared_ptr<atomic_uint64_t> counter, uint32_t dataChunkToSelectPos,
        unordered_set<uint32_t> dataChunksPosInScope, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SKIP, std::move(child), id, paramsString},
          skipNumber{skipNumber}, counter{std::move(counter)},
          dataChunkToSelectPos{dataChunkToSelectPos}, dataChunksPosInScope{
                                                          std::move(dataChunksPosInScope)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Skip>(skipNumber, counter, dataChunkToSelectPos, dataChunksPosInScope,
            children[0]->clone(), id, paramsString);
    }

private:
    uint64_t skipNumber;
    shared_ptr<atomic_uint64_t> counter;
    uint32_t dataChunkToSelectPos;
    shared_ptr<DataChunk> dataChunkToSelect;
    unordered_set<uint32_t> dataChunksPosInScope;
};

} // namespace processor
} // namespace kuzu
