#include "src/processor/include/operator/column_reader/adj_column_property_reader.h"

namespace graphflow {
namespace processor {

void AdjColumnPropertyReader::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    ColumnReader::initialize(graph, morsel);
    column = graph->getRelPropertyColumn(relLabel, nodeLabel, propertyName);
    outValueVector = make_shared<ValueVector>(column->getElementSize());
    outDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
