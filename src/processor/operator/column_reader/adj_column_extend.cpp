#include "src/processor/include/operator/column_reader/adj_column_extend.h"

#include "src/storage/include/structures/column.h"

namespace graphflow {
namespace processor {

void AdjColumnExtend::initialize(Graph* graph) {
    ColumnReader::initialize(graph);
    column = graph->getAdjColumn(direction, nodeLabel, relLabel);
    auto outNodeIDVector = make_shared<NodeIDVector>(
        extensionVariableName, ((AdjColumn*)column)->getCompressionScheme());
    outNodeIDVector->setIsSequence(inNodeIDVector->getIsSequence());
    outValueVector = static_pointer_cast<ValueVector>(outNodeIDVector);
    inDataChunk->append(outValueVector);
    dataChunks->append(inDataChunk);
}

} // namespace processor
} // namespace graphflow
