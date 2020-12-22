#include "src/processor/include/operator/column_reader/node_property_reader.h"

namespace graphflow {
namespace processor {

void NodePropertyReader::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    ColumnReader::initialize(graph, morsel);
    column = graph->getNodePropertyColumn(nodeLabel, propertyName);
    outValueVector = make_shared<ValueVector>(column->getElementSize());
    outDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
