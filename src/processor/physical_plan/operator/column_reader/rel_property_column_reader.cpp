#include "src/processor/include/physical_plan/operator/column_reader/rel_property_column_reader.h"

namespace graphflow {
namespace processor {

RelPropertyColumnReader::RelPropertyColumnReader(uint64_t dataChunkPos, uint64_t valueVectorPos,
    BaseColumn* column, unique_ptr<PhysicalOperator> prevOperator)
    : ColumnReader{dataChunkPos, valueVectorPos, column, move(prevOperator)} {
    outValueVector = make_shared<ValueVector>(column->getDataType());
    inDataChunk->append(outValueVector);
    outValueVector->setDataChunkOwner(inDataChunk);
}

} // namespace processor
} // namespace graphflow
