#include "src/processor/include/operator/physical/column_reader/adj_column_extend.h"

namespace graphflow {
namespace processor {

AdjColumnExtend::AdjColumnExtend(const uint64_t& inDataChunkIdx, const uint64_t& inValueVectorIdx,
    BaseColumn* column, unique_ptr<Operator> prevOperator)
    : ColumnReader{inDataChunkIdx, inValueVectorIdx, column, move(prevOperator)} {
    auto outNodeIDVector = make_shared<NodeIDVector>(((AdjColumn*)column)->getCompressionScheme());
    outNodeIDVector->setIsSequence(inNodeIDVector->getIsSequence());
    outValueVector = static_pointer_cast<ValueVector>(outNodeIDVector);
    inDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
