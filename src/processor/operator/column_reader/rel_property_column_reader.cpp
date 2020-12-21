#include "src/processor/include/operator/column_reader/rel_property_column_reader.h"

namespace graphflow {
namespace processor {

void RelPropertyColumnReader::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    ColumnReader::initialize(graph, morsel);
    column = graph->getRelPropertyColumn(relLabel, nodeLabel, propertyName);
    outValueVector = make_shared<ValueVector>(column->getElementSize());
    outDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
