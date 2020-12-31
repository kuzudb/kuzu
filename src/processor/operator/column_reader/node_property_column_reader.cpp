#include "src/processor/include/operator/column_reader/node_property_column_reader.h"

namespace graphflow {
namespace processor {

void NodePropertyColumnReader::initialize(Graph* graph) {
    ColumnReader::initialize(graph);
    column = graph->getNodePropertyColumn(nodeLabel, propertyName);
    auto name = boundVariableOrRelName + "." + propertyName;
    outValueVector = make_shared<ValueVector>(name, column->getElementSize());
    inDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
