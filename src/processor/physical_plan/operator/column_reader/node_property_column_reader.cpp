#include "src/processor/include/physical_plan/operator/column_reader/node_property_column_reader.h"

#include "src/common/include/vector/operations/vector_comparison_operations.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

NodePropertyColumnReader::NodePropertyColumnReader(uint64_t dataChunkPos, uint64_t valueVectorPos,
    BaseColumn* column, unique_ptr<PhysicalOperator> prevOperator)
    : ColumnReader{dataChunkPos, valueVectorPos, column, move(prevOperator)} {
    outValueVector = make_shared<ValueVector>(column->getDataType());
    inDataChunk->append(outValueVector);
    outValueVector->setDataChunkOwner(inDataChunk);
}

void NodePropertyColumnReader::getNextTuples() {
    ColumnReader::getNextTuples();
    outValueVector->fillNullMask();
}

} // namespace processor
} // namespace graphflow
