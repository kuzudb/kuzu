#include "src/processor/include/physical_plan/operator/column_reader/rel_property_column_reader.h"

namespace graphflow {
namespace processor {

RelPropertyColumnReader::RelPropertyColumnReader(const uint64_t& dataChunkPos,
    const uint64_t& valueVectorPos, BaseColumn* column, unique_ptr<PhysicalOperator> prevOperator)
    : ColumnReader{dataChunkPos, valueVectorPos, column, move(prevOperator)} {
    outValueVector = make_shared<ValueVector>(column->getDataType());
    inDataChunk->append(outValueVector);
}

} // namespace processor
} // namespace graphflow
