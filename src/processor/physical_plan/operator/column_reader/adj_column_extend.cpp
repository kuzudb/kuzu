#include "src/processor/include/physical_plan/operator/column_reader/adj_column_extend.h"

namespace graphflow {
namespace processor {

AdjColumnExtend::AdjColumnExtend(uint64_t dataChunkPos, uint64_t valueVectorPos, BaseColumn* column,
    unique_ptr<PhysicalOperator> prevOperator)
    : ColumnReader{dataChunkPos, valueVectorPos, column, move(prevOperator)} {
    auto outNodeIDVector = make_shared<NodeIDVector>(((AdjColumn*)column)->getCompressionScheme());
    outNodeIDVector->setIsSequence(inNodeIDVector->getIsSequence());
    outValueVector = static_pointer_cast<ValueVector>(outNodeIDVector);
    inDataChunk->append(outValueVector);
    outValueVector->setDataChunkOwner(inDataChunk);
}

} // namespace processor
} // namespace graphflow
