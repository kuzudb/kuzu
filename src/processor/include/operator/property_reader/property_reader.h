#pragma once

#include <memory>

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/operator.h"
#include "src/storage/include/structures/property_column.h"

namespace graphflow {
namespace processor {

template<typename T>
class StructuredNodePropertyReader : public Operator {
protected:
    label_t label;
    uint64_t propertyIdx;
    uint64_t dataChunkIdx;
    shared_ptr<DataChunk> outDataChunk;

    PropertyColumn<T>* column;

    shared_ptr<BaseNodeIDVector> nodeIDVector;
    uint64_t nodeIDVectorIdx;

    shared_ptr<PropertyVector<T>> propertyVector;
    unique_ptr<VectorFrameHandle> handle;

public:
    StructuredNodePropertyReader(
        label_t label, uint64_t propertyIdx, uint64_t nodeVectorIdx, uint64_t dataChunkIdx)
        : label(label), propertyIdx(propertyIdx), dataChunkIdx(dataChunkIdx),
          nodeIDVectorIdx(nodeVectorIdx), handle(new VectorFrameHandle()) {}

    void initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
        prevOperator->initialize(graph, morsel);
        column = static_cast<PropertyColumn<T>*>(graph->getColumn(label, propertyIdx));
        outDataChunks = prevOperator->getOutDataChunks();
        outDataChunk = outDataChunks[dataChunkIdx];
        nodeIDVector =
            static_pointer_cast<BaseNodeIDVector>(outDataChunk->valueVectors[nodeIDVectorIdx]);
        propertyVector = make_shared<PropertyVector<T>>();
        outDataChunk->append(static_pointer_cast<ValueVector>(propertyVector));
    }

    void getNextTuples() {
        column->reclaim(handle.get());
        prevOperator->getNextTuples();
        if (outDataChunk->size > 0) {
            column->readValues(nodeIDVector, propertyVector, outDataChunk->size, handle);
        }
    }

    void cleanup() { column->reclaim(handle.get()); }

    shared_ptr<PropertyVector<T>>& getPropertyVector() { return propertyVector; }
};

} // namespace processor
} // namespace graphflow
