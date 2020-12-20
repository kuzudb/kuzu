#include "src/processor/include/operator/extend/structured_col_extend.h"

namespace graphflow {
namespace processor {

void StructuredColumnExtend::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    PropertyReader::initialize(graph, morsel);
    column = graph->getAdjColumn(direction, srcNodeLabel, relLabel);
    outValueVector = make_shared<ValueVector>(column->getElementSize());
    outValueVector->setIsStoredSequentially(nodeIDVector->isStoredSequentially());
    static_pointer_cast<BaseNodeIDVector>(outValueVector)
        ->setCompressionScheme(column->getCompressionScheme());
    outDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
