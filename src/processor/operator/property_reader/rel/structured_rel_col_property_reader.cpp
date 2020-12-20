#include "src/processor/include/operator/property_reader/rel/structured_rel_col_property_reader.h"

namespace graphflow {
namespace processor {

void StructuredRelColPropertyReader::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    PropertyReader::initialize(graph, morsel);
    column = graph->getRelPropertyColumn(relLabel, nodeLabel, propertyKey);
    outValueVector = make_shared<ValueVector>(column->getElementSize());
    outDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
