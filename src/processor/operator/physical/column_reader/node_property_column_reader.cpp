#include "src/processor/include/operator/physical/column_reader/node_property_column_reader.h"

namespace graphflow {
namespace processor {

NodePropertyColumnReader::NodePropertyColumnReader(const uint64_t& inDataChunkIdx,
    const uint64_t& inValueVectorIdx, BaseColumn* column, unique_ptr<Operator> prevOperator)
    : ColumnReader{inDataChunkIdx, inValueVectorIdx, column, move(prevOperator)} {
    outValueVector = make_shared<ValueVector>(column->getElementSize());
    inDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
