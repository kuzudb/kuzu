#include "src/processor/include/operator/property_reader/node/structured_node_property_reader.h"

namespace graphflow {
namespace processor {

void StructuredNodePropertyReader::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    PropertyReader::initialize(graph, morsel);
    column = graph->getNodePropertyColumn(nodeLabel, propertyKey);
    outValueVector = make_shared<ValueVector>(column->getElementSize());
    outDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
