#pragma once

#include "src/processor/include/physical_plan/hash_table/intersect_hash_table.h"
#include "src/processor/include/physical_plan/operator/sink.h"

namespace graphflow {
namespace processor {

class IntersectBuild : public Sink {
public:
    IntersectBuild(shared_ptr<IntersectHashTable> sharedHT, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString)
        : Sink{move(child), id, paramsString}, mm{nullptr}, sharedHT{move(sharedHT)},
          currentBlockIdx{0} {}

    inline PhysicalOperatorType getOperatorType() override { return INTERSECT_BUILD; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;
    void execute(ExecutionContext* context) override;
    void finalize(ExecutionContext* context) override {}

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<IntersectBuild>(sharedHT, children[0]->clone(), id, paramsString);
    }

private:
    DataBlock* getBlockToAppend(uint32_t numValuesToAppend);
    void append();
    void appendPayloadToBlock(IntersectSlot* slot);

private:
    MemoryManager* mm;
    shared_ptr<IntersectHashTable> sharedHT;
    vector<unique_ptr<DataBlock>> blocks;
    shared_ptr<ValueVector> keyVector;
    shared_ptr<ValueVector> payloadVector;
    uint32_t currentBlockIdx;
};

} // namespace processor
}; // namespace graphflow
