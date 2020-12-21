#include "src/processor/include/operator/column_reader/adj_column_reader.h"

#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace processor {

void AdjColumnReader::initialize(Graph* graph, shared_ptr<MorselDesc>& morsel) {
    ColumnReader::initialize(graph, morsel);
    column = graph->getAdjColumn(direction, nodeLabel, relLabel);
    auto outNodeIDVector = make_shared<NodeIDVector>(((AdjColumn*)column)->getCompressionScheme());
    outNodeIDVector->setIsSequence(nodeIDVector->getIsSequence());
    outValueVector = static_pointer_cast<ValueVector>(outNodeIDVector);
    outDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
