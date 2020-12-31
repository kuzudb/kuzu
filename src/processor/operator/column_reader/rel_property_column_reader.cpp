#include "src/processor/include/operator/column_reader/rel_property_column_reader.h"

namespace graphflow {
namespace processor {

void RelPropertyColumnReader::initialize(Graph* graph) {
    ColumnReader::initialize(graph);
    column = graph->getRelPropertyColumn(relLabel, nodeLabel, propertyName);
    auto name = boundVariableOrRelName + "." + propertyName;
    outValueVector = make_shared<ValueVector>(name, column->getElementSize());
    inDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
