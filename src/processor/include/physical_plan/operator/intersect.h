#pragma once

#include "src/processor/include/physical_plan/hash_table/intersect_hash_table.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Intersect : public PhysicalOperator {

public:
    Intersect(label_t keyLabel, const DataPos& outputVectorPos,
        vector<DataPos> probeSideKeyVectorsPos, vector<shared_ptr<IntersectHashTable>> sharedHTs,
        vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString)
        : PhysicalOperator{move(children), id, paramsString}, keyLabel{keyLabel},
          outputVectorPos{outputVectorPos}, probeSideKeyVectorsPos{move(probeSideKeyVectorsPos)},
          sharedHTs{move(sharedHTs)}, tempResultSize{0}, currentIdxInTempResult{0} {
        probeSideKeyVectors.resize(this->probeSideKeyVectorsPos.size());
        probedLists.resize(this->sharedHTs.size());
    }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;
    bool getNextTuples() override;

    inline PhysicalOperatorType getOperatorType() override { return MW_INTERSECT; }
    inline unique_ptr<PhysicalOperator> clone() override {
        vector<unique_ptr<PhysicalOperator>> clonedChildren;
        for (auto& child : children) {
            clonedChildren.push_back(child->clone());
        }
        return make_unique<Intersect>(keyLabel, outputVectorPos, probeSideKeyVectorsPos, sharedHTs,
            move(clonedChildren), id, paramsString);
    }

private:
    void probe();
    // Left is the smaller one.
    static uint64_t twoWayOffsetIntersect(uint8_t* leftValues, uint64_t leftSize,
        uint8_t* rightValues, uint64_t rightSize, uint8_t* output, label_t keyLabel);
    static uint64_t twoWayNodeIDAndOffsetIntersect(uint8_t* leftValues, uint64_t leftSize,
        uint8_t* rightValues, uint64_t rightSize, uint8_t* output);
    void kWayIntersect();

private:
    label_t keyLabel;
    DataPos outputVectorPos;
    vector<DataPos> probeSideKeyVectorsPos;
    vector<shared_ptr<ValueVector>> probeSideKeyVectors;
    shared_ptr<ValueVector> outputVector;
    vector<shared_ptr<IntersectHashTable>> sharedHTs;
    vector<overflow_value_t> probedLists;
    unique_ptr<nodeID_t[]> tempIntersectedResult;
    uint32_t tempResultSize;
    uint32_t currentIdxInTempResult;
    //    vector<nodeID_t*> currentProbeKeys;
    //    vector<node_offset_t> prefix;
    //    unique_ptr<ValueVector> cachedVector;
    //    shared_ptr<DataChunkState> cachedState;
};

} // namespace processor
} // namespace graphflow
