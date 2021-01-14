#include "src/processor/include/operator/physical/column_reader/rel_property_column_reader.h"

namespace graphflow {
namespace processor {

RelPropertyColumnReader::RelPropertyColumnReader(const uint64_t& dataChunkPos,
    const uint64_t& valueVectorPos, BaseColumn* column, unique_ptr<Operator> prevOperator)
    : ColumnReader{dataChunkPos, valueVectorPos, column, move(prevOperator)} {
    outValueVector = make_shared<ValueVector>(column->getElementSize());
    inDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
