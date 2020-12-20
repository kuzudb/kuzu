#include "src/processor/include/operator/property_reader/node/unstructured_node_property_reader.h"

namespace graphflow {
namespace processor {

void UnstructuredNodePropertyReader::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    PropertyReader::initialize(graph, morsel);
    this->graph = graph;
    outValueVector = make_shared<ValueVector>(sizeof(uint64_t) /* largest possible size */);
    outDataChunk->append(outValueVector);
}

void UnstructuredNodePropertyReader::getNextTuples() {
    column->reclaim(handle);
    prevOperator->getNextTuples();
    if (outDataChunk->size > 0) {
        column = graph->getNodePropertyColumn(nodeIDVector->getLabel(), propertyKey);
        column->readValues(nodeIDVector, outValueVector, outDataChunk->size, handle);
    }
}

} // namespace processor
} // namespace graphflow
